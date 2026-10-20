# Step 0 · Setup and the model (15 min)

**You are on `step-0-setup-solution`: the finished step** (compare:
`git diff origin/step-0-setup origin/step-0-setup-solution -- app`).
Next: `git stash -u`, then `git checkout step-1-connector`.

## Goal
The app runs on your laptop and talks to the model. You understand that one OpenAI-compatible API serves every model,
and that models have **roles**: chat (talks, calls tools), extract (high volume, must return valid JSON), embed
(text → vector).

## Do (everyone)
1. Copy `.env.example` to `.env`. (Only if you'll use the offline story instead of Slack in step 1: also set
   `REPLAY_START` to the start time your instructor gives you, e.g. `REPLAY_START=10:00`. The story's live messages
   are timed from it, so restarting the app keeps their schedule.)
2. `uv run mini-cortex` → open http://127.0.0.1:8787. Stop it with Ctrl+C; restart it after every checkout.
   Memory stays empty for now: no source is wired yet, that's step 1. Chat already answers: it's the model on its
   own, without your data.
3. **Tune → Models**: choose *Scaleway Generative APIs*, paste your API key and project ID, **Load models**,
   pick (the defaults):
   - Chat: `qwen3-235b-a22b-instruct-2507`
   - Extract: `qwen3-235b-a22b-instruct-2507`
   - Embeddings: `qwen3-embedding-8b`, size **1024**
4. **Test connection**: six checks should be green (yellow "stalled" = the provider was slow; the app retries). **Save**.
5. In a second terminal: `uv run doctor`. Same checks, from the command line.

## Explorer (no code)
- Open `.env`: this is all the app knows about your model. The key never leaves your laptop except to the provider.
- In Models, switch the embedding size to 0 (model default) and Test again: how many dims do you get? Why would we
  cut it to 1024? (Hint: Matryoshka embeddings, storage, speed.) Set it back to 1024.

## Builder (optional, if you are fast)
`app/mini_cortex/llm.py` → `extract_native()`: send the JSON schema as `response_format` and validate the reply with
Pydantic. The hints are in the TODO.
**Done when** `uv run doctor` shows `structured output ✔ … json_schema` (until then it says *prompt + parse*), and
the step's tests pass: `uv run --directory app pytest -q -k "extract_native or doctor_all_green"`.

## Why it matters
Everything later (packetize, dedup, chat) is just these three model roles. *Structured output* is the trick that
makes extraction reliable: with guided decoding the server only lets the model write tokens that fit the schema.

## Stuck?
Key rejected → wrong key or project. *TLS / certificate* error on a company laptop → ask your instructor.
Scaleway out of quota or very slow → when your instructor says so, switch to the backup: Models → **OpenRouter
(backup)**, paste the key you're given, **Test connection**, **Save**. No restart, and your memory stays.
Anything else → raise your hand; or `git stash -u`, then `git checkout step-0-setup-solution`.
