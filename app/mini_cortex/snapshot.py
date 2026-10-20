"""Memory snapshots: the data dir (SQLite + LanceDB) as one .tar.gz under <repo>/snapshots/.

A workshop step can start from a prebuilt memory instead of waiting for ingest and packetizing:

    uv run mini-cortex snapshot save day1      # after ingest; the app must be stopped
    uv run mini-cortex snapshot load day1      # the current data moves to <data>.prev first
    uv run mini-cortex snapshot list

Only state.sqlite and lance/ are packed (items.jsonl is a debug log). The manifest records the embedding model and
size: packets from one model are meaningless to another, so loading under different settings warns.
"""

from __future__ import annotations

import io
import json
import shutil
import socket
import sqlite3
import tarfile
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from .llm_config import REPO_ROOT, LLMSettings

SNAPSHOTS_DIR = REPO_ROOT / "snapshots"
MANIFEST = "snapshot.json"
LOCK = "server.lock"
PACKED = ("state.sqlite", "lance")


class SnapshotError(Exception):
    pass


# --- "is the app running on this data dir?" ----------------------------------------------------------------

def write_lock(data: Path, host: str, port: int) -> Path:
    """The server records its address; a lock whose port no longer answers is stale (crash, kill -9)."""
    data.mkdir(parents=True, exist_ok=True)
    path = data / LOCK
    path.write_text(json.dumps({"host": host, "port": port}))
    return path


def running(data: Path) -> str | None:
    try:
        lock = json.loads((data / LOCK).read_text())
        with socket.create_connection((lock["host"], lock["port"]), timeout=0.5):
            return f"http://{lock['host']}:{lock['port']}"
    except (OSError, ValueError, KeyError):
        return None


def refuse_if_running(data: Path) -> None:
    if url := running(data):
        raise SnapshotError(f"mini-cortex is running on this data ({url}); stop it (Ctrl+C) and try again")


# --- save / load / list ------------------------------------------------------------------------------------

def _path(name: str, root: Path) -> Path:
    if not name or any(c in name for c in "/\\:") or name.startswith("."):
        raise SnapshotError(f"bad snapshot name {name!r}: use letters, digits, '-' and '_'")
    return root / f"{name.removesuffix('.tar.gz')}.tar.gz"


def describe(data: Path, settings: LLMSettings) -> dict:
    info: dict = {"embed_model": settings.embed_model, "embed_dim": settings.embed_dim, "items": 0, "packets": 0}
    if (data / "state.sqlite").exists():
        db = sqlite3.connect(data / "state.sqlite")
        try:
            info["items"] = db.execute("select count(*) from raw_items").fetchone()[0]
        except sqlite3.Error:
            pass
        db.close()
    if (data / "lance").exists():
        import lancedb
        ldb = lancedb.connect(data / "lance")
        if "packets" in ldb.list_tables().tables:
            t = ldb.open_table("packets")
            info["packets"] = t.count_rows()
            info["vector_dim"] = t.schema.field("vector").type.list_size
    return info


def save(name: str, data: Path, settings: LLMSettings, root: Path = SNAPSHOTS_DIR) -> tuple[Path, dict]:
    refuse_if_running(data)
    if not (data / "state.sqlite").exists():
        raise SnapshotError(f"nothing to save: no memory in {data}")
    out = _path(name, root)
    root.mkdir(parents=True, exist_ok=True)
    manifest = {"name": out.name.removesuffix(".tar.gz"), "created": datetime.now(UTC).isoformat(timespec="seconds"),
                **describe(data, settings)}
    with tempfile.TemporaryDirectory() as tmp:
        # A consistent copy of SQLite (WAL or not) via the backup API, not a raw file copy.
        src, dst = sqlite3.connect(data / "state.sqlite"), sqlite3.connect(Path(tmp) / "state.sqlite")
        src.backup(dst)
        src.close()
        dst.close()
        partial = out.with_suffix(".partial")
        with tarfile.open(partial, "w:gz") as tar:
            blob = json.dumps(manifest, indent=2).encode()
            ti = tarfile.TarInfo(MANIFEST)
            ti.size, ti.mtime = len(blob), int(datetime.now(UTC).timestamp())
            tar.addfile(ti, io.BytesIO(blob))
            tar.add(Path(tmp) / "state.sqlite", "state.sqlite")
            if (data / "lance").exists():
                tar.add(data / "lance", "lance")
        partial.replace(out)
    return out, manifest


def read_manifest(path: Path) -> dict:
    with tarfile.open(path, "r:gz") as tar:
        f = tar.extractfile(MANIFEST)
        if f is None:
            raise SnapshotError(f"{path.name} has no {MANIFEST}")
        return json.load(f)


def mismatch(manifest: dict, settings: LLMSettings) -> str | None:
    model, dim = manifest.get("embed_model"), manifest.get("vector_dim") or manifest.get("embed_dim")
    if model and settings.embed_model and model != settings.embed_model:
        return (f"snapshot was embedded with {model!r}, your EMBED_MODEL is {settings.embed_model!r}: "
                "search will compare unrelated vectors. Switch the model in Tune → Models, or rebuild memory")
    if dim and settings.embed_dim and dim != settings.embed_dim:
        return (f"snapshot holds {dim}-dim vectors, your EMBED_DIM is {settings.embed_dim}: "
                f"set EMBED_DIM={dim} (Tune → Models), or rebuild memory")
    return None


def load(name: str, data: Path, settings: LLMSettings, root: Path = SNAPSHOTS_DIR) -> tuple[dict, str | None]:
    """Replace the data dir with the snapshot. The previous data is kept once, in <data>.prev."""
    refuse_if_running(data)
    src = _path(name, root)
    if not src.exists():
        raise SnapshotError(f"no snapshot {src.name} in {root} (have: {', '.join(listing(root)) or 'none'})")
    manifest = read_manifest(src)
    staging = data.with_name(data.name + ".loading")
    shutil.rmtree(staging, ignore_errors=True)
    with tarfile.open(src, "r:gz") as tar:
        tar.extractall(staging, members=[m for m in tar.getmembers() if m.name.split("/")[0] in PACKED],
                       filter="data")
    if data.exists():
        prev = data.with_name(data.name + ".prev")
        shutil.rmtree(prev, ignore_errors=True)
        data.replace(prev)
    staging.replace(data)
    return manifest, mismatch(manifest, settings)


def listing(root: Path = SNAPSHOTS_DIR) -> list[str]:
    return sorted(p.name.removesuffix(".tar.gz") for p in root.glob("*.tar.gz"))
