# AGENTS.md

Instructions for coding agents (OpenCode, Claude Code, Codex, ...) working in this repo. People: start with
[README.md](README.md).

## What this is

mini-cortex is a workshop app, a personal memory assistant. Data sources (connectors) serve and push messages; the
app turns them into typed knowledge packets (packetize), merges duplicates (dedup), stores them in LanceDB and answers
questions with hybrid search and citations (chat).

```
connectors/<name>_adapter.py ──WebSocket──▶ hub ──▶ pipeline ──▶ SQLite + LanceDB (.data/)
      (child processes)                       └──── API + prebuilt UI on http://127.0.0.1:8787
```

The workshop is split into branches: `step-0-setup` … `step-4-chat`, each with a `-solution`, then `step-5-live`, the
complete app. On a step branch the `TODO(workshop:builder)` functions are the participant's exercise: don't fill them
in unless you are asked to.

## Commands (repo root)

```bash
uv run mini-cortex --connector replay    # app + the offline replay story; Ctrl+C stops everything (no source by default)
uv run mini-cortex --connector replay --connector notes    # the story plus connectors/notes_adapter.py
uv run doctor                                       # checks the model settings in .env
uv run --directory app pytest -q                    # app tests: a stub model server, no key needed
uv run --directory connectors pytest -q             # connector tests
uv run --directory connectors python notes_adapter.py fetch --days 7    # dry run: print items, no hub
```

## Writing a connector (the usual task here)

1. Create `connectors/<name>_adapter.py`, lowercase name. The app finds connectors by this file name.
2. Subclass `SourceAdapter` from `connectors/source_adapter.py`. Set `name`, `display_name` and
   `streams = [StreamSpec("<stream>", kind=...)]`, where `kind` is `chat`, `email`, `note`, `issue`, `event` or
   `other`. Use `time_filter="native"` when the source itself filters by time.
3. Implement the four duties:
   - `authenticate()`: log in, find what to read. Raise with a clear message when a setting is missing.
   - `to_item(stream, native)`: one native object → an IngestItem dict, or `None` to skip it.
   - `fetch_window(stream, since, until)`: every item with `since <= timestamp < until` (half-open). Sorting,
     paging and cursors are done by the base class.
   - `watch(emit)`: optional live push. `await emit(stream, items)` returns once the hub stored them; only then
     advance your position with `self.save_cursor(...)`.
4. IngestItem fields (full schema: PROTOCOL.md §5):
   - required: `source_id`, `timestamp`, `content`. `source_id` must be stable (the same object always gets the same
     id, e.g. `<stream>:<native id>`; never a content hash). `timestamp` is the event time in UTC, written with
     `connector_sdk.iso()`. `content` is the text the model reads; for mail put From/To/Subject in it, as
     `replay_adapter.py` does.
   - recommended: `author` (`{"name", "id", "email"}`), `thread_id` (the conversation: channel, mail thread, note
     path), `reply_to`, `title`, `url`, `participants`, `metadata` (for chats: `channel`, `channel_id`).
5. End the file with `if __name__ == "__main__": main(MyAdapter)`. That gives it the `serve` and `fetch` commands.
6. Settings and secrets come from environment variables. People put them in `connectors/.env`, which `main()` loads
   and git ignores; the app also passes on settings from the repo-root `.env`. Never print or log a secret, never
   commit a `.env`, never hardcode a token or an endpoint.
7. Dependencies: the standard library, `httpx` and `websockets` are there. For anything else:
   `uv add --directory connectors <package>`.
8. Add `connectors/tests/test_<name>_adapter.py` in the style of `tests/test_replay_adapter.py`: windows are
   half-open, ids stay stable across calls, `dry_fetch` pages, and one run against `dev_hub.DevHub`.

Copy the shape of `replay_adapter.py` (the simplest complete example: files → items, a native time filter, timed live
pushes) or `slack_adapter.py` (a real API: tokens, paging, threads, polling and Socket Mode).

## Rules

- Don't change the wire protocol (`PROTOCOL.md`, `connectors/connector_sdk.py`) to make a connector work.
- `app/mini_cortex/hub_base.py` holds a copy of `DevHub` from `connectors/dev_hub.py`: change both or neither
  (`app/tests/test_hub.py` compares them).
- The UI in `app/mini_cortex/static/` is prebuilt. Change `web/src`, then `uv run --directory app python
  scripts/build_ui.py` (needs Node) and commit the result.
- Model settings live in the repo-root `.env` (Tune → Models writes it). Code reads them through
  `mini_cortex.llm_config.load_settings()`.
- After changing the memory pipeline, run the tests, then the golden questions against the running app:
  `uv run python app/scripts/golden.py --url http://127.0.0.1:8787`. For search or chat changes, load the full memory
  first (`uv run mini-cortex snapshot load herdlink-full`). For extraction, packet types or dedup changes, load it,
  then press Rebuild memory and Process (Memory screen) so the packets come from your code: that re-runs the whole
  story, about 800 model calls.
