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
| `uv run mini-cortex --connector slack` | also start `connectors/slack_adapter.py` (repeatable) |
| `uv run mini-cortex --port 8797 --hub-port 8798` | other ports, e.g. when 8787 is taken |
| `uv run mini-cortex snapshot load NAME` | start from a prebuilt memory in `snapshots/` (stop the app first) |
| `uv run mini-cortex --help` | all options |

More: [app/README.md](app/README.md) (the app), [connectors/README.md](connectors/README.md) (sources),
[web/README.md](web/README.md) (UI development), [PROTOCOL.md](PROTOCOL.md) (connector protocol).
