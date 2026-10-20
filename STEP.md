# Step 2 · Packetize: from messages to knowledge (20 min)

**You are on `step-2-packetize`.** Finished version: `git stash -u`, then `git checkout step-2-packetize-solution`.
Next: `git stash -u`, then `git checkout step-3-dedup`.

**Explorers:** work on `step-2-packetize-solution` (on this branch Process stops at the unfinished functions).

## Goal
An LLM with **structured output** turns noisy messages into small, typed, self-contained facts (**packets**) that
keep a link to the messages they came from.

## Concepts
- **Packet**: one piece of durable knowledge: `type`, `title`, `content`, `entities`, `sources`. Memory stores
  packets, not chat logs.
- **Taxonomy as config**: the types live in `app/memory_types.yaml` (fact, event, decision, person, preference,
  reference, warning, commitment). The prompt's type list and the JSON schema's `type` enum are generated from it.
  Change the YAML → change what your assistant remembers.
- **Durability test**: "will this still be useful in two weeks?" Greetings, bot noise and newsletters fail it.
- **Windows**: messages are processed per conversation, a few at a time; earlier messages are shown as context only.
- **Provenance**: the model cites `[m3]`-style refs, so every packet keeps its original messages. Click a source in
  Memory to read the original; that is how you catch the model's mistakes.

## Explorer (no code)
1. Memory → **Process**. Read the PACKETIZE rows: which messages went in, which packets came out.
2. Packets tab: open packets, click their sources, compare with the original. Find one the model got wrong.
3. Tune: switch off `preference` (or edit `memory_types.yaml`), **Rebuild memory**, **Process** again. What changed?
4. What did the model make of the AgriTech newsletter? A newsletter fails the durability test, and this one hides
   a prompt injection: at most a warning about the hidden instruction should come out of it. What did yours make?

## Builder
`app/mini_cortex/packetize.py`, three TODOs (hints in the code):
1. **The rules** in `SYSTEM_PROMPT`: write the rules that make packets good (one fact each, split bundled tasks,
   absolute dates, short titles, entities).
2. **`packet_schema()`**: the Pydantic schema; `type` must be one of the enabled types (`Literal[...]`).
3. **`extract()`**: call `llm.extract(...)` and turn each packet into a draft with its `sources`.

While a TODO is missing, **Process** stops and says which function to write.
**Done when** Process makes packets, each shows its original messages, and
`uv run --directory app pytest -q tests/test_pipeline.py` is green.

Try it: Process with only the mechanical rules first, then add your rules, Rebuild, Process, compare packets.

## Waiting for the model?
Processing the whole story takes a while. Process the slice you have, or skip ahead with a full memory:
`uv run mini-cortex snapshot load herdlink-full` (stop the app first, then start it again).

## Why it matters
Retrieval is only as good as what you stored. Small, self-contained, typed packets with sources are what make the
chat answers in step 4 precise and checkable.

## Stuck?
`git stash -u`, then `git checkout step-2-packetize-solution` and restart the app.
