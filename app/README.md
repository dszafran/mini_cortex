# mini-cortex app

One process, one command (PLAN §3): the connector hub, the memory pipeline, the API and the prebuilt UI.

```
 connectors/<name>_adapter.py ──ws──▶ hub (:8765) ──▶ pipeline ──▶ SQLite + LanceDB (.data/)
   (child processes)                     │                              │
                                         └──── API + UI (:8787) ◀───────┘
```

## Run

From the repo root or from `app/` (the root `pyproject.toml` makes `app/` a uv workspace member: one `uv.lock` and one
`.venv`, both at the root):

```bash
uv run mini-cortex                                  # UI and API on http://127.0.0.1:8787, hub on :8765
uv run mini-cortex --connector slack                # also start connectors/slack_adapter.py (repeatable)
MINI_CORTEX_CONNECTORS=replay,slack uv run mini-cortex
uv run mini-cortex --backfill-days 30 --data /tmp/other-memory --port 8797 --hub-port 8798
uv run mini-cortex snapshot save day1               # app stopped; packs .data into <repo>/snapshots/day1.tar.gz
uv run mini-cortex snapshot load day1               # replaces .data (the old one is kept in .data.prev)
uv run mini-cortex snapshot list
```

- **Connectors** run as child processes (`uv run --project ../connectors python ../connectors/<name>_adapter.py serve`
  with `HUB_URL` and `CONNECTOR_TOKEN` set; their own settings such as `SLACK_BOT_TOKEN` come from your environment).
  Their logs are prefixed `[name]`; a crashed one is restarted up to 3 times; Ctrl+C stops them all. Default: `replay`
  (the offline story) when `connectors/replay_adapter.py` exists, otherwise none. `--connector none` or
  `MINI_CORTEX_CONNECTORS=` starts none. A connector started by hand still dials into `ws://127.0.0.1:8765/connectors`.
- **Backfill**: on a connector's first hello the hub fetches the last `--backfill-days` (default 60; 0 = none). Fetched
  items wait as *pending* (Memory → Process); pushed items are packetized after the buffer window.
- **Snapshots** record the embedding model and size; loading under different settings warns, since the vectors would
  not match. Save and load refuse while the app runs on that data dir.
- Do **not** run a second Slack adapter with the same `SLACK_APP_TOKEN` as another session: Socket Mode spreads events
  across connections, so each adapter would see only some messages. Polling (no app token) is safe.

| File | What |
|---|---|
| `mini_cortex/server.py` | `uv run mini-cortex`: CLI, FastAPI app (`/api/health`, `/api/connectors`, `/api/frames`, `/api/items`, `/api/fetch`, `/api/events`), static UI |
| `mini_cortex/hub.py` | The hub: records every protocol frame for the wire log, connector state, intake log, feeds the pipeline |
| `mini_cortex/hub_base.py` | Hub side of the protocol, a copy of `connectors/dev_hub.py` (`tests/test_hub.py` fails when they drift) |
| `mini_cortex/launcher.py` | Connector child processes: start, prefix logs, restart, stop |
| `mini_cortex/snapshot.py` | `snapshot save/load/list` |
| `mini_cortex/pipeline.py`, `packetize.py`, `dedup.py`, `store.py`, `search.py`, `chat.py`, `pipeline_routes.py` | Memory: items → packets → dedup → LanceDB; search and chat |
| `mini_cortex/llm_config.py` | Settings from the repo-root `.env` (`MINI_CORTEX_ENV` overrides the path). Provider presets: Scaleway, Ollama, any OpenAI-compatible URL |
| `mini_cortex/llm.py` | `LLM`: one `openai` client for every role. `chat`, `stream`, `extract` (structured output with a prompt+parse fallback), `embed` (Cortex's `search_document:` / `search_query:` prefixes, `dimensions`) |
| `mini_cortex/doctor.py` | `uv run doctor`: settings, model list, chat, structured output, tool calling, embedding size. Each failure prints a fix |
| `mini_cortex/llm_routes.py` | `/api/llm/*` for Tune → Models |
| `mini_cortex/static/` | The prebuilt UI (`uv run python scripts/build_ui.py` refreshes it; see `../web/README.md`) |

## Configure

Either use the UI (Tune → Models: pick a provider, paste the key, **Load models**, **Test connection**, **Save**), or
copy `../.env.example` to `../.env` and fill it in.

```bash
uv run doctor          # green = ready for the workshop
uv run doctor --json   # same checks, machine-readable
uv run pytest          # stub OpenAI-compatible server, a real SDK connector end to end, launcher, snapshots; no key needed
```

Scaleway uses `SCW_SECRET_KEY` and `SCW_PROJECT_ID`, the same variables OpenCode's Scaleway provider reads, so one key
serves both. The base URL is `https://api.scaleway.ai/<project id>/v1`. Any other OpenAI-compatible server (a vLLM
deployment, Ollama, LM Studio, OpenAI) works with `LLM_PROVIDER=custom` + `LLM_BASE_URL` + `LLM_API_KEY`.

## The key

- It is stored only in the local `.env` (mode 600). The API never returns it: `GET /api/llm/settings` gives
  `key_set` plus the last 4 characters.
- Saving without a key keeps the stored one. **Load models** and **Test connection** use the unsaved form values
  (falling back to the stored key), so you can try a key before saving it.
- Real environment variables win over `.env`. The UI lists any that do, so a save never silently does nothing.

## Use it from code

```python
from mini_cortex.llm import LLM
from mini_cortex.llm_config import load_settings

llm = LLM(load_settings())
msg = await llm.chat([{"role": "user", "content": "hi"}])                 # CHAT_MODEL
packets = await llm.extract(messages, PacketList)                          # EXTRACT_MODEL → validated Pydantic
vectors = await llm.embed(["text"], kind="document")                       # EMBED_MODEL, EMBED_DIM
```
