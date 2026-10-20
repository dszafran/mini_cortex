"""Hybrid search over packets: vector similarity + full text, fused with Reciprocal Rank Fusion.

RRF (K=60, as in Cortex): every result list contributes 1 / (K + rank) for each packet it contains.
A packet near the top of both lists wins; one that only matches words, or only meaning, still gets a chance.
LanceDB has a built-in hybrid query too; writing RRF by hand shows what it does.
"""

from __future__ import annotations

from .llm import LLM
from .store import PacketStore

RRF_K = 60


def rrf(*ranked: list[dict], k: int = RRF_K) -> list[dict]:
    # TODO(workshop:builder) step 4 (1/3): Reciprocal Rank Fusion of the vector results and the full-text results.
    # Each argument is a list of packets, best first. For every list and every packet at position rank (1, 2, ...):
    #       score[packet id] += 1 / (k + rank)
    # A packet found by both lists gets two contributions and rises to the top. Return the packets (each once),
    # highest score first, with the score attached as packet | {"_rrf": score}.
    # Until then: only the vector results are used, so exact names and IDs ("AT-4471", "4.2.2") are often missed.
    # Stretch: compare with LanceDB's built-in hybrid query + RRFReranker.
    return list(ranked[0]) if ranked else []


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
