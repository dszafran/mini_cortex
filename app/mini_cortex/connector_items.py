"""Small helpers for IngestItems (PROTOCOL.md §5) as the pipeline sees them."""

from __future__ import annotations

from datetime import UTC, datetime


def parse_ts(s: str) -> datetime:
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def short_time(s: str) -> str:
    """'2026-10-14T15:02:00.000Z' → '2026-10-14 15:02' (UTC; the model resolves relative dates against it)."""
    return parse_ts(s).astimezone(UTC).strftime("%Y-%m-%d %H:%M")


def source_ref(row: dict, label: str) -> dict:
    """A raw row as the UI's SourceRef."""
    it = row["item"]
    return {"source_id": it["source_id"], "connector": row["connector"], "stream": row["stream"],
            "timestamp": it["timestamp"], "author": (it.get("author") or {}).get("name"), "label": label,
            "url": it.get("url")}
