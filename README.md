# mini-cortex

A personal memory assistant you build in the workshop: **sources → packets → dedup → memory → chat**.

## Quick start

You need **git** and **[uv](https://docs.astral.sh/uv/getting-started/installation/)**. Nothing else: no Docker, no
Node, no system Python (uv fetches Python 3.12+ itself). Works the same on macOS, Windows and Linux.

```bash
git clone <repo-url> mini-cortex
cd mini-cortex
uv run doctor          # checks your model key and settings; green = ready
uv run mini-cortex     # the app: open http://127.0.0.1:8787
```

The model key: copy `.env.example` to `.env` (repo root) and fill in `SCW_SECRET_KEY`, or start the app and use
**Tune → Models** (paste the key, **Test connection**, **Save**). Then run `uv run doctor` again.

`uv run mini-cortex` starts everything in one process: the connector hub, the memory pipeline, the API and the UI, plus
the connectors you enable (`--connector replay`, `--connector slack`, ...). Stop it with Ctrl+C. Both commands work
from the repo root and from `app/`: the root `pyproject.toml` is a uv workspace around `app/`, with one `.venv`.

| Command | What |
|---|---|
| `uv run mini-cortex --connector slack` | start `connectors/slack_adapter.py` instead of the replay story (repeatable: `--connector replay --connector slack`) |
| `uv run mini-cortex --port 8797 --hub-port 8798` | other ports, e.g. when 8787 is taken |
| `uv run mini-cortex snapshot load NAME` | start from a prebuilt memory in `snapshots/` (stop the app first) |
| `uv run mini-cortex --help` | all options |

More: [app/README.md](app/README.md) (the app), [connectors/README.md](connectors/README.md) (sources),
[web/README.md](web/README.md) (UI development), [PROTOCOL.md](PROTOCOL.md) (connector protocol),
[AGENTS.md](AGENTS.md) (for coding agents).

## Workshop (Fri 23 Oct 2026)

Six steps, one branch each. Every step branch has a `STEP.md`: **Builders** write the missing function, **Explorers**
use the finished version of the step (`…-solution`) and explore it without code.

| Step | Branch | Min |
|---|---|---|
| 0 · Setup and the model | `step-0-setup` | 15 |
| 1 · The universal connector | `step-1-connector` | 20 |
| 2 · Packetize | `step-2-packetize` | 20 |
| 3 · Dedup on write | `step-3-dedup` | 20 |
| 4 · RAG chat with citations | `step-4-chat` | 30 |
| 5 · Live memory and red team | `step-5-live` | 10 |

- **Before the day:** install git and uv, clone, then run `uv sync` and `uv sync --project connectors`. That downloads
  everything once (about 450 MB). The model key comes from Scaleway during the workshop.
- **Start:** copy `.env.example` to `.env`, run `git checkout step-0-setup` and open `STEP.md`.
- **Next step:** `git stash -u`, then `git checkout <next branch>`. Your own work stays in `git stash list`.
- **Stuck, or exploring:** steps 0-4 have a finished version, e.g. `git checkout step-2-packetize-solution` (stash
  first). Step 5 is the complete app already.
- **After every checkout:** stop the app with Ctrl+C and start it again; Python doesn't pick up code changes.
- **If Scaleway runs out of quota**, the instructor hands out a backup key: Tune → Models → **OpenRouter (backup)**,
  paste the key, **Test connection**, **Save**. No restart, and your memory stays: same models, same embeddings.
- `main` is the finished app.
