"""Hybrid search over packets: vector similarity + full text, fused with Reciprocal Rank Fusion.

RRF (K=60, as in Cortex): every result list contributes 1 / (K + rank) for each packet it contains.
A packet near the top of both lists wins; one that only matches words, or only meaning, still gets a chance.
LanceDB has a built-in hybrid query too; writing RRF by hand shows what it does.
"""

from __future__ import annotations

from .llm import LLM
from .store import PacketStore

RRF_K = 60


# EXPLORE: compare this hand-written RRF with LanceDB's built-in hybrid query + RRFReranker.
def rrf(*ranked: list[dict], k: int = RRF_K) -> list[dict]:
    scores: dict[str, float] = {}
    first_seen: dict[str, dict] = {}
    for results in ranked:
        for rank, p in enumerate(results, 1):
            scores[p["id"]] = scores.get(p["id"], 0.0) + 1.0 / (k + rank)
            first_seen.setdefault(p["id"], p)
    return [first_seen[i] | {"_rrf": s} for i, s in sorted(scores.items(), key=lambda kv: -kv[1])]


def where_clause(type_: str | None, date_from: str | None, date_to: str | None) -> str:
    parts = ["deprecated = false"]
    if type_:
        parts.append(f"type = '{type_.replace(chr(39), '')}'")
    if date_from:
        parts.append(f"source_timestamp >= '{date_from[:10]}'")
    if date_to:
        parts.append(f"source_timestamp < '{date_to[:10]}T99'")   # inclusive day
    return " and ".join(parts)


async def hybrid_search(llm: LLM, packets: PacketStore, query: str, limit: int = 8, type_: str | None = None,
                        date_from: str | None = None, date_to: str | None = None) -> list[dict]:
    """`type_` is a preference, not a filter (as in Cortex): models often guess the type wrong, so a third of the
    slots go to that type and the rest to the best matches of any type."""
    [qvec] = await llm.embed([query], kind="query")

    def run(where: str, k: int) -> list[dict]:
        return rrf(packets.vector_search(qvec, k * 2, where), packets.text_search(query, k * 2, where))[:k]

    anything = run(where_clause(None, date_from, date_to), limit)
    if not type_:
        return anything
    preferred = run(where_clause(type_, date_from, date_to), max(1, limit // 3))
    seen = {p["id"] for p in preferred}
    return (preferred + [p for p in anything if p["id"] not in seen])[:limit]
