"""Where memory lives: SQLite for state (raw items, memory log, chat threads), LanceDB for packets (vectors + full text).

Both are plain files under MINI_CORTEX_DATA (default `<repo>/.data/`). No server, nothing to install.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import uuid
from datetime import UTC, datetime
from pathlib import Path

import lancedb
import pyarrow as pa
from lancedb.index import FTS

from .llm_config import REPO_ROOT


def now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def data_dir() -> Path:
    return Path(os.environ.get("MINI_CORTEX_DATA") or REPO_ROOT / ".data")


def conversation_key(connector: str, item: dict) -> str:
    """Messages of one conversation are packetized together: a Slack channel/DM, a mail thread, ..."""
    meta = item.get("metadata") or {}
    return f"{connector}:{meta.get('channel_id') or item.get('thread_id') or item['source_id']}"


def content_hash(item: dict) -> str:
    return hashlib.sha1(json.dumps([item.get("content"), item.get("updated_at")], sort_keys=True).encode()).hexdigest()


SCHEMA = """
create table if not exists raw_items (
    connector text not null, source_id text not null, stream text not null, conv text not null,
    ts text not null, item text not null, hash text not null,
    status text not null default 'pending',          -- pending | packetized | error
    error text, updated text not null,
    primary key (connector, source_id));
create index if not exists raw_status on raw_items (status, conv, ts);
create table if not exists memory_log (
    seq integer primary key autoincrement, id text unique not null, at text not null, stage text not null,
    action text not null, title text not null, reason text, packet_ids text, source text, detail text);
create table if not exists chat_threads (id text primary key, title text not null, updated text not null);
create table if not exists chat_messages (
    seq integer primary key autoincrement, id text not null, thread_id text not null, role text not null,
    content text not null, steps text, citations text, created text not null);
"""


class StateDB:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)

    # --- raw items --------------------------------------------------------------------------------------

    def upsert_raw(self, connector: str, stream: str, items: list[dict]) -> list[dict]:
        """Store items; returns those that are new or changed (they go back to `pending`)."""
        changed = []
        for it in items:
            h = content_hash(it)
            row = self.db.execute("select hash from raw_items where connector=? and source_id=?",
                                  (connector, it["source_id"])).fetchone()
            if row and row["hash"] == h:
                continue
            self.db.execute(
                "insert into raw_items (connector, source_id, stream, conv, ts, item, hash, status, updated) "
                "values (?,?,?,?,?,?,?, 'pending', ?) on conflict (connector, source_id) do update set "
                "stream=excluded.stream, conv=excluded.conv, ts=excluded.ts, item=excluded.item, "
                "hash=excluded.hash, status='pending', error=null, updated=excluded.updated",
                (connector, it["source_id"], stream, conversation_key(connector, it), it["timestamp"],
                 json.dumps(it), h, now()))
            changed.append(it)
        return changed

    def pending(self, conv: str | None = None) -> list[dict]:
        """Items to process: new ones, and ones whose last attempt failed (a stalled call, a bad reply)."""
        q = ("select * from raw_items where status in ('pending', 'error')" + (" and conv=?" if conv else "")
             + " order by conv, ts")
        return [self._raw(r) for r in self.db.execute(q, (conv,) if conv else ())]

    def raw_item(self, connector: str, source_id: str) -> dict | None:
        r = self.db.execute("select * from raw_items where connector=? and source_id=?",
                            (connector, source_id)).fetchone()
        return self._raw(r) if r else None

    def context_before(self, conv: str, ts: str, n: int) -> list[dict]:
        rows = self.db.execute("select * from raw_items where conv=? and ts<? and status='packetized' "
                               "order by ts desc limit ?", (conv, ts, n)).fetchall()
        return [self._raw(r) for r in reversed(rows)]

    def mark(self, keys: list[tuple[str, str]], status: str, error: str | None = None) -> None:
        self.db.executemany("update raw_items set status=?, error=?, updated=? where connector=? and source_id=?",
                            [(status, error, now(), c, s) for c, s in keys])

    def reset_all_pending(self) -> None:
        self.db.execute("update raw_items set status='pending', error=null")

    def raw_counts(self) -> dict[str, int]:
        return {r["status"]: r["n"] for r in self.db.execute("select status, count(*) n from raw_items group by status")}

    @staticmethod
    def _raw(r: sqlite3.Row) -> dict:
        return {"connector": r["connector"], "stream": r["stream"], "conv": r["conv"], "item": json.loads(r["item"]),
                "status": r["status"]}

    # --- memory log -------------------------------------------------------------------------------------

    def log(self, entry: dict) -> dict:
        entry = {"id": entry.get("id") or new_id("ml"), "at": entry.get("at") or now(), **entry}
        self.db.execute("insert into memory_log (id, at, stage, action, title, reason, packet_ids, source, detail) "
                        "values (?,?,?,?,?,?,?,?,?)",
                        (entry["id"], entry["at"], entry["stage"], entry["action"], entry["title"],
                         entry.get("reason"), json.dumps(entry.get("packet_ids") or []),
                         json.dumps(entry.get("source")), json.dumps(entry.get("detail"))))
        return entry

    def memory_log(self, limit: int = 500) -> list[dict]:
        rows = self.db.execute("select * from memory_log order by seq desc limit ?", (limit,))
        return [{"id": r["id"], "at": r["at"], "stage": r["stage"], "action": r["action"], "title": r["title"],
                 "reason": r["reason"], "packet_ids": json.loads(r["packet_ids"] or "[]"),
                 "source": json.loads(r["source"] or "null"), "detail": json.loads(r["detail"] or "null")}
                for r in rows]

    def log_counts(self) -> dict[tuple[str, str], int]:
        return {(r["stage"], r["action"]): r["n"]
                for r in self.db.execute("select stage, action, count(*) n from memory_log group by stage, action")}

    def clear_pipeline_log(self) -> None:
        self.db.execute("delete from memory_log where stage in ('packetize', 'dedup')")

    # --- chat -------------------------------------------------------------------------------------------

    def threads(self) -> list[dict]:
        return [dict(r) for r in self.db.execute("select * from chat_threads order by updated desc")]

    def touch_thread(self, thread_id: str, title: str) -> None:
        self.db.execute("insert into chat_threads (id, title, updated) values (?,?,?) on conflict (id) do update "
                        "set updated=excluded.updated", (thread_id, title, now()))

    def add_message(self, thread_id: str, msg: dict) -> None:
        self.db.execute("insert into chat_messages (id, thread_id, role, content, steps, citations, created) "
                        "values (?,?,?,?,?,?,?)",
                        (msg["id"], thread_id, msg["role"], msg["content"], json.dumps(msg.get("steps") or []),
                         json.dumps(msg.get("citations") or []), now()))

    def messages(self, thread_id: str) -> list[dict]:
        return [{"id": r["id"], "role": r["role"], "content": r["content"], "steps": json.loads(r["steps"] or "[]"),
                 "citations": json.loads(r["citations"] or "[]")}
                for r in self.db.execute("select * from chat_messages where thread_id=? order by seq", (thread_id,))]


class DimensionMismatch(RuntimeError):
    pass


PACKET_FIELDS = ("id", "type", "title", "content", "entities", "sources", "source_timestamp", "created_at",
                 "updated_at", "deprecated", "superseded_by", "access_count")


def _esc(s: str) -> str:
    return s.replace("'", "''")


class PacketStore:
    """Packets in LanceDB: one row per packet, `vector` = embedding of the content (titles are not embedded)."""

    def __init__(self, path: Path, dim: int = 0):
        """`dim` 0 = whatever the first vector has (the embedding model's default size)."""
        self.db = lancedb.connect(path)
        self.dim = dim
        self.table = None
        self._fts = False
        if "packets" in self.db.list_tables().tables:
            self.table = self.db.open_table("packets")
            have = self.table.schema.field("vector").type.list_size
            if dim and have != dim:
                raise DimensionMismatch(f"memory holds {have}-dim vectors but the embedding size is now {dim}; "
                                        "rebuild memory to re-embed")
            self.dim = have

    def __len__(self) -> int:
        return self.table.count_rows() if self.table is not None else 0

    def _create(self, dim: int) -> None:
        schema = pa.schema([
            pa.field("id", pa.string()), pa.field("type", pa.string()), pa.field("title", pa.string()),
            pa.field("content", pa.string()), pa.field("entities", pa.list_(pa.string())),
            pa.field("sources", pa.string()),   # JSON list of SourceRef
            pa.field("source_timestamp", pa.string()), pa.field("created_at", pa.string()),
            pa.field("updated_at", pa.string()), pa.field("deprecated", pa.bool_()),
            pa.field("superseded_by", pa.string()), pa.field("access_count", pa.int64()),
            pa.field("vector", pa.list_(pa.float32(), dim))])
        self.table = self.db.create_table("packets", schema=schema, exist_ok=True)
        self.dim = dim

    def ensure_fts(self) -> None:
        if not self._fts and len(self):
            self.table.create_index("content", config=FTS(), replace=True)
            self._fts = True

    def upsert(self, packet: dict, vector: list[float]) -> None:
        row = {k: packet.get(k) for k in PACKET_FIELDS}
        row["sources"] = json.dumps(packet.get("sources") or [])
        row["entities"] = list(packet.get("entities") or [])
        row["superseded_by"] = packet.get("superseded_by") or ""
        row["access_count"] = int(packet.get("access_count") or 0)
        row["deprecated"] = bool(packet.get("deprecated"))
        row["vector"] = vector
        if self.table is None:
            self._create(len(vector))
        elif len(vector) != self.dim:
            raise DimensionMismatch(f"memory holds {self.dim}-dim vectors, this embedding has {len(vector)}; "
                                    "rebuild memory to re-embed")
        self.table.merge_insert("id").when_matched_update_all().when_not_matched_insert_all().execute([row])

    def get(self, packet_id: str) -> dict | None:
        if not len(self):
            return None
        rows = self.table.search().where(f"id = '{_esc(packet_id)}'").limit(1).to_list()
        return self._packet(rows[0]) if rows else None

    def all(self, include_deprecated: bool = True) -> list[dict]:
        if not len(self):
            return []
        q = self.table.search()
        if not include_deprecated:
            q = q.where("deprecated = false")
        rows = q.limit(len(self)).to_list()
        return sorted((self._packet(r) for r in rows), key=lambda p: p["source_timestamp"], reverse=True)

    def neighbours(self, vector: list[float], type_: str, k: int) -> list[tuple[dict, float]]:
        """Same-type, live packets nearest to `vector`, with cosine similarity."""
        if not len(self):
            return []
        rows = (self.table.search(vector).metric("cosine")
                .where(f"type = '{_esc(type_)}' and deprecated = false", prefilter=True).limit(k).to_list())
        return [(self._packet(r), 1 - r["_distance"]) for r in rows]

    def vector_search(self, vector: list[float], k: int, where: str | None) -> list[dict]:
        if not len(self):
            return []
        q = self.table.search(vector).metric("cosine").limit(k)
        if where:
            q = q.where(where, prefilter=True)
        return [self._packet(r) | {"_cosine": 1 - r["_distance"]} for r in q.to_list()]

    def text_search(self, query: str, k: int, where: str | None) -> list[dict]:
        if not len(self):
            return []
        self.ensure_fts()
        q = self.table.search(query, query_type="fts").limit(k)
        if where:
            q = q.where(where, prefilter=True)
        try:
            return [self._packet(r) for r in q.to_list()]
        except Exception:  # noqa: BLE001 — odd query syntax must not break chat; the vector leg still answers
            return []

    def set_sources(self, packet_id: str, sources: list[dict]) -> None:
        self.table.update(where=f"id = '{_esc(packet_id)}'", values={"sources": json.dumps(sources)})

    def bump_access(self, ids: list[str]) -> None:
        if not len(self):
            return
        for pid in ids:
            self.table.update(where=f"id = '{_esc(pid)}'", values_sql={"access_count": "access_count + 1"})

    def count(self, where: str | None = None) -> int:
        return self.table.count_rows(where) if self.table is not None else 0

    def clear(self) -> None:
        """Drop the table: a rebuild may use a different embedding size."""
        if self.table is not None:
            self.db.drop_table("packets")
        self.table, self._fts = None, False

    @staticmethod
    def _packet(r: dict) -> dict:
        p = {k: r.get(k) for k in PACKET_FIELDS}
        p["sources"] = json.loads(r.get("sources") or "[]")
        p["entities"] = list(r.get("entities") or [])
        p["superseded_by"] = r.get("superseded_by") or None
        return p
