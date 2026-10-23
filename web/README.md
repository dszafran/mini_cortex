# mini-cortex web UI

The participant-facing UI: **Chat · Memory · Sources · Tune**.

```bash
npm install
npm run dev      # http://localhost:5173  (open #chat, #memory, #sources or #tune)
npm run build    # → dist/; the app serves it when it is newer than the shipped copy
```

## Shipping a new build

Participants have no Node: the app serves the copy committed in `app/mini_cortex/static/`. After UI changes, run

```bash
cd ../app && uv run python scripts/build_ui.py    # npm ci + npm run build here, then copy dist/ → static/
```

and commit `app/mini_cortex/static/`. Until then, a local `web/dist` that is newer than the shipped copy is served
instead, so `npm run build` alone is enough on your own machine.

## Layout

| Path | What |
|---|---|
| `src/api/types.ts` | Data shapes shared with the future backend (packets, memory_log, connectors, wire frames, chat events) |
| `src/api/mock.ts` | Placeholder data from the Lighthouse story bible. Replace with real fixtures |
| `src/api/client.ts` | The only data entry point. Swap mock calls for `fetch()`/SSE here; screens stay unchanged |
| `src/screens/*` | One file per screen |
| `src/styles/tokens.css` | Theme tokens: dark by default, light under `[data-theme="light"]` |

## Live mode

When the app (`uv run mini-cortex` in `../app`) is reachable at `/api`, the top bar shows **live hub** and every screen
shows real connectors, frames, memory and chat. `npm run dev` proxies `/api` to it on :8787. Otherwise the UI shows
**demo data**.

## Mock behaviour

- Chat streams canned answers. Try asking about the backhaul backup, your promises, Gregor's reports, or whether the small cell is cancelled.
- In Memory, θ follows the **applied** value from Tune, so the line between the nearest packets moves after you press Apply.
- In Sources, "Backfill a period…" appends a `fetch` frame to the wire log.
