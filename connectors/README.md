# connectors

The universal connector interface for mini-cortex, and the adapters built on it. Implements
[`../PROTOCOL.md`](../PROTOCOL.md) v1.1 (v1.0 + the optional `coverage` field).

| File | What it is |
|---|---|
| `source_adapter.py` | **The interface.** `SourceAdapter` ABC with the four duties (authenticate, `to_item`, `fetch_window`/`fetch`, `watch`), `StreamSpec`, the runner, and a shared CLI (`serve` / `fetch` dry run) |
| `connector_sdk.py` | Wire protocol client: WebSocket, `hello`, JSON-RPC, acked push, reconnect, state file |
| `slack_adapter.py` | Slack, with a bot token (public channels → `channels`) or a user token (your channels → `channels`, your DMs → `dms`). Native time-window fetch incl. thread replies; push via Socket Mode or polling |
| `replay_adapter.py` | The Herdlink story from `fixtures/herdlink/` (`channels`, `dms`, `mail`, `notes`), time-shifted to today, with the live cues pushed on a schedule. Offline, no setup |
| `fixtures/` | The story data (`herdlink/herdlink.jsonl`, `herdlink/cues.jsonl`) and its format (`dataset.schema.json`, `dataset.py`) |
| `slack_manifest.yaml` | Slack app manifest: bot + user scopes, events, Socket Mode |
| `dev_hub.py` | Fake hub for testing. The app vendors its `DevHub` as `app/mini_cortex/hub_base.py`: change both (an app test checks) |
| `tests/` | `uv run pytest`: fake Slack Web API + Socket Mode, run against the dev hub |

## Writing an adapter

```python
from source_adapter import SourceAdapter, StreamSpec, main

class Notes(SourceAdapter):
    name = "notes"
    streams = [StreamSpec("notes", kind="note")]

    async def authenticate(self): ...                       # 1. log in / find the folder
    async def to_item(self, stream, native): ...            # 2. -> {"source_id", "content", "timestamp", ...}
    async def fetch_window(self, stream, since, until): ... # 3. all items in [since, until); paging is done for you
    async def watch(self, emit): ...                        # 4. await emit(stream, items); then self.save_cursor(...)

if __name__ == "__main__":
    main(Notes)
```

Optional: override `fetch(...)` for native paging, `coverage(...)` for partial history, set `fetch=False` on a
push-only stream.

## Slack

```bash
uv sync
cp .env.example .env                                   # set SLACK_TOKEN (and SLACK_APP_TOKEN for Socket Mode)
uv run python slack_adapter.py fetch --days 30         # dry run, prints IngestItems as JSONL, no hub needed
uv run python dev_hub.py serve --backfill-days 30      # terminal 1
uv run python slack_adapter.py serve                   # terminal 2; post in Slack and watch terminal 1
uv run python dev_hub.py fetch --connector slack --stream channels --days 7
```

Token type is detected from the prefix (`SLACK_TOKEN_TYPE` overrides it):

| | bot `xoxb-` | user `xoxp-` |
|---|---|---|
| Streams | `channels` | `channels`, `dms` |
| Reads | every public channel (joins it) | only conversations the person is in, incl. private channels, DMs, group DMs; never joins anything |
| Good for | a shared demo workspace | "my own Slack": take-home, participant-owned workspaces |

A bot can never read a DM between two people, whatever its scopes; only the people in it can.
New channels and DMs are picked up while running (conversation list refreshed every 60 s, or on an unknown event).

Mapping: `source_id = <stream>:<conversation id>:<ts>`, `thread_id` = channel name (`dm-<name>` for DMs, which
also carry `participants`), `reply_to` = thread parent,
`author` = user's real name, or the persona `username` for bot posts made with `chat:write.customize`.
`<@U…>`, `<#C…>` and `<url|label>` are rendered as text. Joins, topic changes and other system messages are skipped.

Known limits:
- Deletes are not propagated (protocol v1).
- In polling mode, an edit to a message older than the poll overlap (2 min) is not re-pushed. Socket Mode catches it.
- Replies in threads whose parent is older than `SLACK_THREAD_LOOKBACK_DAYS` before the window are not fetched.

## Replay (offline story)

```bash
uv run python replay_adapter.py fetch --days 60                 # dry run: channels (or --stream dms|mail|notes)
uv run python dev_hub.py serve --backfill-days 60               # terminal 1
uv run python replay_adapter.py serve                           # terminal 2; Enter pushes the next cue now
REPLAY_SPEED=20 uv run python replay_adapter.py serve           # rehearsal: the 105-minute cue script in ~5 min
```

The story is written relative to D0. `REPLAY_D0` (default: today in Europe/Vienna) maps it to a real day, keeping
times of day; `{D+2}`-style dates in the text are rendered as real dates. Items in the future stay hidden.

Live cues (`cues.jsonl`, `minute` after the start) are pushed at `REPLAY_START + minute / REPLAY_SPEED`.
`REPLAY_START` is an ISO datetime or `HH:MM` on D0 (default: when the adapter starts). Set it on workshop day so a
restart keeps the schedule; without it a restart replays the cues from minute 0 (same `source_id`s, so the hub
counts them as updates). `REPLAY_CUES`: `timed` (default; Enter or `kill -USR1 <pid>` pulls the next cue forward),
`manual` (only on Enter / SIGUSR1), `all-now`, `off`.

Slices for quick steps: `REPLAY_STREAMS=channels,mail`, `REPLAY_CHANNELS=noc-incidents,farmers` (also filters the
cues), `REPLAY_LIMIT=40` (only the newest 40 story items).

Mapping: `source_id = <stream>:<conversation>:<record id>` (`channels:product:a-01`, `dms:dm-freya:ab-f03`,
`mail:inbox:b-01`, `notes:work/todo.md:b-09`). Channels and DMs carry `metadata.channel_id` (one conversation each,
like Slack); mail is threaded by `thread_id` (the root subject) with `reply_to` from `in_reply_to`; each note is its
own conversation (`thread_id` = path). No `url`: there is nothing to open.
