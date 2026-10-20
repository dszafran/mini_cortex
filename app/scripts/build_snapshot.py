"""Rebuild the prebuilt memory: the whole replay story, ingested and processed as of workshop day D0.

    uv run python scripts/build_snapshot.py                          # D0 from REPLAY_D0, saves herdlink-full
    uv run python scripts/build_snapshot.py --d0 2026-10-23 --parallel 3

Participants load it with `uv run mini-cortex snapshot load herdlink-full` instead of waiting for the model. The
story's dates are rendered against D0, so a snapshot only fits a workshop on that day: rebuild it when the date moves.

It starts the app on a scratch data dir with the replay clock pinned to 07:00 on D0 and no live cues, fetches every
stream, presses Process until nothing is pending (failed windows are retried), stops the app and saves
snapshots/<name>.tar.gz. It uses your model settings (.env, or MINI_CORTEX_ENV). A full run is about 800 model calls:
on a shared endpoint, lower --parallel.
"""

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import httpx
import yaml

from mini_cortex import snapshot
from mini_cortex.llm_config import REPO_ROOT, env_path, load_settings, read_env_file
from mini_cortex.memory_config import types_path

STREAMS = ("channels", "dms", "mail", "notes")
STORY = REPO_ROOT / "connectors" / "fixtures" / "herdlink" / "herdlink.jsonl"
# The whole story whatever the .env says: an empty value beats the app's .env (real environment variables win there).
NO_SLICES = {k: "" for k in ("REPLAY_START", "REPLAY_SPEED", "REPLAY_STREAMS", "REPLAY_CHANNELS", "REPLAY_LIMIT",
                             "REPLAY_DATASET", "REPLAY_CUE_FILE")}
WINDOWS = sys.platform == "win32"
BUILD_DIR = REPO_ROOT / ".data.build"           # ignored by git (.data.*/)
ROUNDS = 4                                       # Process again while windows fail (rate limits, stalls)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for(check, timeout: float, what: str):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            if result := check():
                return result
        except httpx.HTTPError:
            pass
        time.sleep(1)
    sys.exit(f"timed out waiting for {what}; app log: {BUILD_DIR / 'app.log'}")


def process_until_done(api: httpx.Client) -> dict:
    for round_ in range(1, ROUNDS + 1):
        api.post("/api/pipeline/process")
        time.sleep(2)
        while (s := api.get("/api/stats").json())["progress"]["state"] == "running":
            p = s["progress"]
            print(f"  round {round_}: {p['done']}/{p['total']} windows, {s['packets']} packets", end="\r", flush=True)
            time.sleep(5)
        print()
        if s["progress"]["state"] == "failed":
            sys.exit(f"Process failed: {s['progress']['error']}")
        if not s["pending"] and not s["errors"]:
            return s
        print(f"  {s['pending']} pending, {s['errors']} failed: processing again")
    sys.exit(f"items still pending after {ROUNDS} rounds; app log: {BUILD_DIR / 'app.log'}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--d0", default=os.environ.get("REPLAY_D0") or read_env_file(env_path()).get("REPLAY_D0"),
                    help="workshop day, YYYY-MM-DD (default: REPLAY_D0)")
    ap.add_argument("--name", default="herdlink-full")
    ap.add_argument("--parallel", type=int, help="extraction calls in flight (default: memory_types.yaml)")
    args = ap.parse_args()
    if not args.d0:
        sys.exit("say which day the snapshot is for: --d0 YYYY-MM-DD (or set REPLAY_D0)")
    d0 = date.fromisoformat(args.d0)
    settings = load_settings()
    if missing := settings.missing():
        sys.exit(f"model settings incomplete ({', '.join(missing)}): run `uv run doctor`")

    data = BUILD_DIR / "data"
    if url := snapshot.running(data):
        sys.exit(f"another build is running on {data} ({url})")
    shutil.rmtree(BUILD_DIR, ignore_errors=True)
    data.mkdir(parents=True)
    story = sum(json.loads(line)["kind"] == "item" for line in STORY.read_text(encoding="utf-8").splitlines() if line.strip())
    env = {**os.environ, **NO_SLICES, "REPLAY_D0": d0.isoformat(), "REPLAY_NOW": f"{d0.isoformat()}T07:00",
           "REPLAY_CUES": "off"}
    if args.parallel:
        cfg = yaml.safe_load(types_path().read_text())
        cfg["pipeline"]["parallel_windows"] = args.parallel
        (BUILD_DIR / "memory_types.yaml").write_text(yaml.safe_dump(cfg, sort_keys=False, allow_unicode=True))
        env["MINI_CORTEX_TYPES"] = str(BUILD_DIR / "memory_types.yaml")
    port, hub_port = free_port(), free_port()
    started = time.monotonic()
    with (BUILD_DIR / "app.log").open("w") as log:
        app = subprocess.Popen([sys.executable, "-m", "mini_cortex.server", "--data", str(data), "--port", str(port),
                                "--hub-port", str(hub_port), "--backfill-days", "0", "--connector", "replay"],
                               env=env, stdout=log, stderr=subprocess.STDOUT,
                               creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if WINDOWS else 0)
    api = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=60)
    try:
        wait_for(lambda: any(c["name"] == "replay" and c["status"] == "live"
                             for c in api.get("/api/connectors").json()["connectors"]), 120, "the replay connector")
        since, until = (d0 - timedelta(days=60)).isoformat(), (d0 + timedelta(days=1)).isoformat()
        items = {s: api.post("/api/fetch", json={"connector": "replay", "stream": s, "since": f"{since}T00:00:00Z",
                                                 "until": f"{until}T00:00:00Z"}).json()["items"] for s in STREAMS}
        print(f"fetched {sum(items.values())} items: {items}")
        if sum(items.values()) != story:
            sys.exit(f"fetched {sum(items.values())} of the story's {story} items: is fetch_window() written "
                     "(step 1), or is a REPLAY_* slice set?")
        stats = process_until_done(api)
    finally:
        app.send_signal(signal.CTRL_BREAK_EVENT if WINDOWS else signal.SIGINT)   # Ctrl+C: stops its connector too
        app.wait(30)
    path, manifest = snapshot.save(args.name, data, settings)
    minutes = (time.monotonic() - started) / 60
    print(f"saved {path} ({path.stat().st_size / 1e6:.1f} MB) in {minutes:.0f} min: {manifest['items']} items, "
          f"{manifest['packets']} packets, {stats['merged']} merged, {stats['skipped_known']} skipped as known")
    shutil.rmtree(BUILD_DIR, ignore_errors=True)


if __name__ == "__main__":
    main()
