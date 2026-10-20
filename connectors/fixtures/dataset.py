"""Load, validate and time-anchor a workshop dataset (JSONL, see dataset.schema.json).

    uv run --project datasets python datasets/dataset.py validate datasets/herdlink/herdlink.jsonl
    uv run --project datasets python datasets/dataset.py anchor datasets/herdlink/herdlink.jsonl --anchor 2026-11-20
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from jsonschema import Draft202012Validator

SCHEMA_PATH = Path(__file__).with_name("dataset.schema.json")


class DatasetError(ValueError):
    pass


@dataclass
class Dataset:
    meta: dict
    personas: dict[str, dict] = field(default_factory=dict)
    channels: dict[str, dict] = field(default_factory=dict)
    items: list[dict] = field(default_factory=list)
    cues: list[dict] = field(default_factory=list)

    @property
    def tz(self) -> ZoneInfo:
        return ZoneInfo(self.meta["timezone"])

    def email_of(self, persona_id: str) -> str:
        p = self.personas[persona_id]
        return p.get("email") or f"{p['username']}@{self.meta['email_domain']}"

    def channels_with_mode(self, mode: str) -> list[dict]:
        return [c for c in self.channels.values() if c["mode"] == mode]

    def chat_personas(self) -> list[dict]:
        return [p for p in self.personas.values() if p.get("in_chat", True)]


def load(path: str | Path) -> Dataset:
    """Parse and validate a dataset file. Raises DatasetError listing every problem found."""
    validator = Draft202012Validator(json.loads(SCHEMA_PATH.read_text()))
    errors: list[str] = []
    lines: list[dict] = []
    for n, raw in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), start=1):
        if not raw.strip():
            continue
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as e:
            errors.append(f"line {n}: invalid JSON ({e.msg})")
            continue
        schema_errors = list(validator.iter_errors(obj))
        if schema_errors:
            # oneOf errors are noisy; report the line and the kind/source to make it findable.
            errors.append(f"line {n}: does not match any {obj.get('kind')}/{obj.get('source', '-')} shape")
            continue
        lines.append(obj)
    if errors:
        raise DatasetError("\n".join(errors))

    metas = [o for o in lines if o["kind"] == "meta"]
    if len(metas) != 1:
        raise DatasetError(f"expected exactly one meta line, found {len(metas)}")
    ds = Dataset(meta=metas[0])
    for o in lines:
        match o["kind"]:
            case "persona":
                ds.personas[o["id"]] = o
            case "channel":
                ds.channels[o["id"]] = o
            case "item":
                ds.items.append(o)
            case "cue":
                ds.cues.append(o)
    ds.cues.sort(key=lambda c: c["order"])
    _check_references(ds)
    return ds


def _check_references(ds: Dataset) -> None:
    errors: list[str] = []
    ids = Counter([i["id"] for i in ds.items] + [c["id"] for c in ds.cues])
    errors += [f"duplicate id {i!r}" for i, n in ids.items() if n > 1]
    usernames = Counter(p["username"] for p in ds.personas.values())
    errors += [f"duplicate username {u!r}" for u, n in usernames.items() if n > 1]
    if ds.meta["protagonist"] not in ds.personas:
        errors.append(f"protagonist {ds.meta['protagonist']!r} is not a persona")
    try:
        ZoneInfo(ds.meta["timezone"])
    except Exception:
        errors.append(f"unknown timezone {ds.meta['timezone']!r}")

    in_mm = {p["id"] for p in ds.chat_personas()}
    by_id = {i["id"]: i for i in ds.items}
    dm_channels = ds.channels_with_mode("dm_archive")
    if len(dm_channels) > 1:
        errors.append("at most one channel may have mode dm_archive")

    for it in ds.items:
        src = it["source"]
        if src in ("chat", "chat_dm") and it["author"] not in in_mm:
            errors.append(f"{it['id']}: author {it['author']!r} is not a chat persona")
        if src == "chat":
            ch = ds.channels.get(it["channel"])
            if ch is None:
                errors.append(f"{it['id']}: unknown channel {it['channel']!r}")
            elif ch["mode"] != "history":
                errors.append(f"{it['id']}: channel {it['channel']!r} is {ch['mode']}; backdated posts go to history channels")
            if "reply_to" in it:
                root = by_id.get(it["reply_to"])
                if root is None or root["source"] != "chat" or root["channel"] != it["channel"]:
                    errors.append(f"{it['id']}: reply_to must reference a post in the same channel")
        if src == "chat_dm":
            if not dm_channels:
                errors.append(f"{it['id']}: DM items need a channel with mode dm_archive")
            missing = [p for p in it["participants"] if p not in ds.personas]
            if missing or it["author"] not in it["participants"]:
                errors.append(f"{it['id']}: participants must be personas and include the author")
        if src == "email" and "in_reply_to" in it and it["in_reply_to"] not in by_id:
            errors.append(f"{it['id']}: in_reply_to {it['in_reply_to']!r} not found")

    for cue in ds.cues:
        ch = ds.channels.get(cue["channel"])
        if ch is None or ch["mode"] != "live":
            errors.append(f"cue {cue['id']}: channel must be a live channel")
        if cue["author"] not in in_mm:
            errors.append(f"cue {cue['id']}: author {cue['author']!r} is not a chat persona")
    if errors:
        raise DatasetError("\n".join(errors))


def anchor_times(ds: Dataset, anchor: date) -> dict[str, datetime]:
    """Map item id -> UTC datetime, with D0 = `anchor` in the dataset's timezone.

    Items sharing the same day+time get +1 s, +2 s ... in file order, so timestamps are unique and the
    original order is preserved (Slack import and (timestamp, source_id) ordering both rely on it).
    """
    tz = ds.tz
    seen: Counter[tuple[int, str]] = Counter()
    out: dict[str, datetime] = {}
    for it in ds.items:
        key = (it["day"], it["time"])
        out[it["id"]] = local_to_utc(anchor, it["day"], it["time"], tz) + timedelta(seconds=seen[key])
        seen[key] += 1
    return out


def local_to_utc(anchor: date, day: int, hhmm: str, tz: ZoneInfo) -> datetime:
    h, m = (int(x) for x in hhmm.split(":"))
    local = datetime.combine(anchor + timedelta(days=day), time(h, m), tzinfo=tz)
    return local.astimezone(UTC)


def to_millis(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    v = sub.add_parser("validate", help="validate a dataset file")
    v.add_argument("path")
    a = sub.add_parser("anchor", help="print items with real UTC timestamps")
    a.add_argument("path")
    a.add_argument("--anchor", type=date.fromisoformat, default=date.today(), help="date of D0 (default: today)")
    args = ap.parse_args()
    try:
        ds = load(args.path)
    except DatasetError as e:
        sys.exit(f"INVALID:\n{e}")
    if args.cmd == "validate":
        by_source = Counter(i["source"] for i in ds.items)
        print(f"OK: {ds.meta['dataset']} - {len(ds.personas)} personas, {len(ds.channels)} channels, "
              f"{len(ds.items)} items {dict(by_source)}, {len(ds.cues)} cues")
    else:
        times = anchor_times(ds, args.anchor)
        for it in sorted(ds.items, key=lambda i: times[i["id"]]):
            print(f"{times[it['id']].isoformat().replace('+00:00', 'Z')}  {it['source']:<14} {it['id']}")


if __name__ == "__main__":
    _main()
