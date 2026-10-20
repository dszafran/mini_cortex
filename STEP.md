# Step 1 · The universal connector (20 min)

**You are on `step-1-connector`.** Finished version: `git stash -u`, then `git checkout step-1-connector-solution`.
Next: `git stash -u`, then `git checkout step-2-packetize`.

**Everyone** wires Slack below. **Explorers** then explore on `step-1-connector-solution`; the Builder task is about
the offline story's connector, whose history fetch isn't written yet on this branch.

## Goal
Data sources are **separate processes** that speak one small protocol with the app. Any source fits, Slack, mail,
notes, WhatsApp, if you can (1) map one native message to an item and (2) answer "give me this time window".

## Start: wire your Slack (everyone)
Until now no source was wired, so memory is empty. Your instructor gives you a Slack workspace with the story in it.
1. https://api.slack.com/apps → **Create New App** → **From a manifest** → pick your workshop workspace → paste the
   contents of `connectors/slack_manifest.yaml` → **Create** → **Install to Workspace** → **Allow**.
2. **OAuth & Permissions** → copy the **User OAuth Token** (`xoxp-…`): it reads your channels *and* your DMs.
3. **Basic Information** → **App-Level Tokens** → **Generate**, scope `connections:write` → copy it (`xapp-…`): new
   messages arrive the moment they're posted. Without it the app polls every 20 s.
4. Add three lines to `.env` (never commit it):
   ```
   MINI_CORTEX_CONNECTORS=slack
   SLACK_TOKEN=xoxp-…
   SLACK_APP_TOKEN=xapp-…
   ```
5. Restart `uv run mini-cortex`. The start-up line says `connectors: slack`. Open http://127.0.0.1:8787 →
   **Sources**: the story's channels fill up.
6. Post a message in any channel of your workspace and watch it arrive (Sources → **Wire**: `push`, then `ack`).

**No Slack, or stuck?** Use the offline story: `MINI_CORTEX_CONNECTORS=replay` in `.env` (and `REPLAY_START`, see
step 0), restart. Same story, plus mail and notes; its live messages arrive on a schedule.

## Concepts (see `PROTOCOL.md`)
- **hello**: the connector dials in and lists its streams. Slack: `channels` (and `dms` with a user token); the
  offline story: `channels`, `dms`, `mail`, `notes`.
- **fetch(stream, since, until, cursor, limit)**: the app asks for history (backfill). Big windows come back in
  **pages**; the **cursor** says where to continue.
- **push**: the connector sends new items as they happen; the app **acks** them.
- **IngestItem**: `source_id` (stable, so storing twice is harmless), `timestamp` (when it happened), `content`,
  `author`, `thread_id`, `url`.

## Explorer (no code)
1. Sources → **Wire**: find `hello`, the backfill `fetch` pages (look at `has_more` and `cursor`), the `result`s.
2. Your own Slack message (offline story: at ~0:18 **Brunhilde** heads for the Swiss border): find the `push` and its
   `ack`. How long did it take to arrive?
3. **Backfill a period**: fetch only the last 7 days of `channels`. What changes in Wire (pages, `cursor`)?
4. Items tab: pick an item, read its JSON. Which fields would *your* favourite source fill?

## Builder
`connectors/replay_adapter.py` → `fetch_window()` (hints in the TODO): return every story item of the stream whose
timestamp is in `[since, until)` and not in the future. Run it next to Slack: `MINI_CORTEX_CONNECTORS=slack,replay`.
- Try it without the app: `uv run --directory connectors python replay_adapter.py fetch --days 7`
- **Done when** `uv run --directory connectors pytest -q tests/test_replay_adapter.py` is all green, and after restarting
  `uv run mini-cortex` Sources shows the offline story (~230 items) next to your Slack.
- **Stretch:** write an adapter for a source you use (notes folder, mail, WhatsApp export): copy the shape of
  `replay_adapter.py`, implement the four duties from `source_adapter.py` (authenticate, `to_item`,
  `fetch_window`, `watch`). OpenCode can scaffold it from `AGENTS.md`.

## Why it matters
The app never knows where data comes from. That is what lets one assistant read Slack today and your mail tomorrow,
and why history (fetch) and live (push) are separate: you need both to answer "what happened last Tuesday?".

## Stuck?
`git stash -u`, then `git checkout step-1-connector-solution`, restart `uv run mini-cortex`, and keep exploring Sources.
