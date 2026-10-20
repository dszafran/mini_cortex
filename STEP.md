# Step 3 · Dedup on write (20 min)

**You are on `step-3-dedup`.** Finished version: `git stash -u`, then `git checkout step-3-dedup-solution`.
Next: `git stash -u`, then `git checkout step-4-chat`.

**Explorers:** work on `step-3-dedup-solution` (this branch adds every packet, so there are no MERGE or SKIP decisions
to look at).

## Goal
Memory should not fill up with the same fact five times. **Embeddings** and a **similarity threshold** find
candidate duplicates cheaply; an **LLM judge** decides: ADD (new knowledge), MERGE (same subject, new detail) or
SKIP (nothing new).

## Concepts
- **Embedding**: text → vector. Similar meaning → small angle → **cosine similarity** close to 1.
  We use 1024 dimensions (Matryoshka models can be cut down with little loss).
- **θ (theta)**: below θ the packet is added without asking anyone (cheap). At or above θ the model judges (one
  call, but catches duplicates). θ lives in `app/memory_types.yaml` and on the Tune screen.
- **LLM as judge**: a verdict prompt + structured output (`Verdict`: reason, action, target_id, title, content).
  Every field is required and length-limited, so a looping model can't silently drop or bloat a merge.
- **Near-duplicates that must NOT merge**: firmware 4.2.1 (APN fallback) vs 4.2.2 (clock drift); Brunhilde's first
  wander (€4,180) vs her second (€15, the cap held).

## Start (everyone)
After the checkout and restart: **Memory → Rebuild memory**, then **Process**. Dedup runs only when a packet is
written, so the packets from step 2 (stored without dedup) are not checked. The rebuild forgets them and sends every
item through packetize and dedup again. It takes a few minutes, and everyone does it at once, so slow progress or a
"rate limited …; waiting N s, then trying again" line in the terminal is normal.

## Explorer (no code)
1. Memory → Activity → filter **MERGE** and **SKIP**. Open one: the neighbours with their cosines, the θ line, the
   model's reason, the text before and after.
2. Tune θ to **0.75** and Apply. In the MERGE and SKIP rows, which neighbours sit between 0.75 and 0.86? Those pairs
   would now go to the judge: would it merge them correctly? The next live messages go through at the new θ. Then
   **0.92**: what slips through? (To see a new θ on the whole story, Rebuild memory + Process again: only if
   your instructor says the quota allows.)
3. At ~0:58 the live message about **fw 4.2.3** arrives: it must stay a separate packet. Does it?

## Builder
`app/mini_cortex/dedup.py` → `dedup()` (step-by-step hints in the TODO). The helpers `add()`, `merge_sources()`,
`union_sources()` and `sane_merge()` are ready to use. Until you finish, every packet is simply added. After your change, Rebuild memory + Process once more.
**Done when** Activity shows MERGE/SKIP rows with readable reasons, 4.2.1 and 4.2.2 stay separate packets, and
`uv run --directory app pytest -q tests/test_pipeline.py` is green.

## Why it matters
Without dedup, memory grows with every repeated message and search returns five copies of the same thing. With a
judge that's too eager, two different facts become one wrong fact. θ is where you trade cost against recall.

## Stuck?
`git stash -u`, then `git checkout step-3-dedup-solution` and restart the app.
