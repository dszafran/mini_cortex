"""Pipeline, dedup, search and chat with a fake LLM: deterministic, no key, no network."""

import hashlib
import math
import re
from types import SimpleNamespace

import pytest

from mini_cortex.chat import Chat, cited_numbers
from mini_cortex.llm_config import LLMSettings
from mini_cortex.memory_config import load_memory_config
from mini_cortex.packetize import build_windows, render
from mini_cortex.pipeline import Pipeline
from mini_cortex.search import rrf

DIM = 64


def bow(text: str) -> list[float]:
    """Bag of words hashed into DIM buckets: shared words → high cosine."""
    v = [0.0] * DIM
    for w in re.findall(r"[a-z0-9]+", text.lower()):
        v[int(hashlib.md5(w.encode()).hexdigest(), 16) % DIM] += 1
    n = math.sqrt(sum(x * x for x in v)) or 1
    return [x / n for x in v]


class FakeLLM:
    """Extract: one fact per [mN] message (except 'ok'/'thanks'). Verdict: SKIP identical, MERGE if 'update', else ADD."""

    def __init__(self, chat_script=None):
        self.s = SimpleNamespace(chat_model="fake-chat")
        self.chat_script = chat_script or []
        self.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=self._create)))
        self.calls = []

    async def embed(self, texts, kind="document"):
        return [bow(t) for t in texts]

    async def extract(self, messages, schema, **kwargs):
        prompt = messages[-1]["content"]
        if schema.__name__ == "PacketBatch":
            new = prompt.split("--- New messages ---", 1)[1]
            packets = []
            for ref, text in re.findall(r"\[(m\d+)\] \S+ \S+ [^:]+: (.*)", new):
                if text.strip().lower() in ("ok", "thanks"):
                    continue
                packets.append({"type": "fact", "title": " ".join(text.split()[:5]), "content": text,
                                "entities": [], "source_refs": [ref]})
            return schema.model_validate({"packets": packets})
        new = re.search(r"content: (.*)", prompt.split("EXISTING:")[0])[1]
        target = re.search(r"id: (\S+)", prompt.split("EXISTING:")[1])[1]
        old = re.search(r"content: (.*)", prompt.split("EXISTING:")[1])[1]
        if new == old:
            return schema(action="SKIP", target_id=target, title="", content="", reason="same")
        if "update" in new:
            return schema(action="MERGE", target_id=target, title="", content=f"{old} (update: {new})",
                          reason="newer detail")
        return schema(action="ADD", target_id="", title="", content="", reason="different")

    async def _create(self, **kw):
        self.calls.append(kw)
        script = self.chat_script.pop(0)

        async def gen():
            for part in script:
                if isinstance(part, str):
                    yield SimpleNamespace(choices=[SimpleNamespace(delta=SimpleNamespace(content=part, tool_calls=None))])
                else:
                    tc = SimpleNamespace(index=0, id="c1", function=SimpleNamespace(name=part[0], arguments=part[1]))
                    yield SimpleNamespace(choices=[SimpleNamespace(delta=SimpleNamespace(content=None, tool_calls=[tc]))])
        return gen()


def item(n, text, minute, channel="C1", name="firmware"):
    return {"source_id": f"channels:{channel}:{n}", "timestamp": f"2026-10-14T10:{minute:02d}:00.000Z",
            "content": text, "author": {"name": "pim"}, "thread_id": name,
            "metadata": {"channel": name, "channel_id": channel, "channel_type": "public"}}


@pytest.fixture
def pipe(tmp_path):
    cfg = load_memory_config()
    cfg.pipeline.theta = 0.8
    events = []
    p = Pipeline(LLMSettings(provider="custom", base_url="http://x/v1", embed_dim=DIM), data=tmp_path,
                 llm=FakeLLM(), cfg=cfg, publish=lambda e, d: events.append(e))
    p.events = events
    return p


def test_windows_split_on_gap_and_render_refs(pipe):
    pipe.cfg.pipeline.window_gap_minutes = 15
    pipe.ingest("slack", "channels", [item(1, "a", 0), item(2, "b", 5), item(3, "c", 40)], live=False)
    windows = build_windows(pipe.state.pending(), pipe.cfg)
    assert [len(w.rows) for w in windows] == [2, 1]          # 35 min gap > 15
    text, refs = render(windows[0])
    assert "[m1] 2026-10-14 10:00 pim: a" in text and set(refs) == {"m1", "m2"}


async def test_process_adds_skips_and_merges(pipe):
    pipe.ingest("slack", "channels", [
        item(1, "Adaptive GPS beta gives seven days of battery", 0),
        item(2, "ok", 1),
        item(3, "Adaptive GPS beta gives seven days of battery", 2),            # same → SKIP
        item(4, "Adaptive GPS beta gives seven days of battery update to 7.5", 3),  # → MERGE
        item(5, "Brunhilde crossed into Switzerland again", 4)], live=False)
    await pipe.process()
    actions = [e["action"] for e in reversed(pipe.state.memory_log()) if e["stage"] == "dedup"]
    assert actions == ["ADD", "SKIP", "MERGE", "ADD"]
    packets = pipe.packets.all()
    assert len(packets) == 2
    gps = next(p for p in packets if "GPS" in p["title"])
    assert "7.5" in gps["content"] and len(gps["sources"]) == 3              # provenance kept through SKIP + MERGE
    s = pipe.stats()
    assert (s["packetized"], s["pending"], s["merged"], s["skipped_known"], s["packets"]) == (5, 0, 1, 1, 2)
    assert {"memory", "progress", "stats"} <= set(pipe.events)


async def test_a_failing_window_does_not_stop_the_run_and_is_retried(pipe, monkeypatch):
    real = pipe.llm.embed

    async def flaky(texts, kind="document"):
        if any("boom" in t for t in texts):
            raise TimeoutError("stalled")
        return await real(texts, kind)

    monkeypatch.setattr(pipe.llm, "embed", flaky)
    pipe.ingest("slack", "channels", [item(1, "boom goes the dedup call", 0),
                                      item(2, "Skyrill migration starts Saturday", 0, channel="C2", name="noc")],
                live=False)
    await pipe.process()
    s = pipe.stats()
    assert (s["errors"], s["packets"], s["progress"]["state"]) == (1, 1, "done")
    monkeypatch.setattr(pipe.llm, "embed", real)
    await pipe.process()                                    # Process retries the failed item
    assert (pipe.stats()["errors"], pipe.stats()["packets"]) == (0, 2)


async def test_unreachable_model_stops_the_run_and_keeps_items_pending(pipe, monkeypatch):
    import httpx
    import openai

    async def down(messages, schema, **kw):
        raise openai.APIConnectionError(request=httpx.Request("POST", "http://x/v1/chat/completions"))

    monkeypatch.setattr(pipe.llm, "extract", down)
    pipe.ingest("slack", "channels", [item(1, "a", 0), item(2, "b", 0, channel="C2", name="noc")], live=False)
    await pipe.process()
    s = pipe.stats()
    assert s["progress"]["state"] == "failed" and "Cannot reach" in s["progress"]["error"]
    assert (s["pending"], s["errors"]) == (2, 0)          # nothing marked as failed: Process again once it's fixed


async def test_edited_item_goes_back_to_pending_and_rebuild(pipe):
    pipe.ingest("slack", "channels", [item(1, "Notary on Tue 27 Oct at 15:00", 0)], live=False)
    await pipe.process()
    assert pipe.ingest("slack", "channels", [item(1, "Notary on Tue 27 Oct at 15:00", 0)], live=False) == 0
    assert pipe.ingest("slack", "channels", [item(1, "Notary moved to Wed 28 Oct", 0)], live=False) == 1
    await pipe.rebuild()
    assert pipe.stats()["packets"] == 0 and pipe.stats()["pending"] == 1


async def test_live_buffer_flushes_after_n_messages(pipe):
    pipe.cfg.pipeline.buffer_messages = 2
    pipe.start()
    pipe.ingest("slack", "channels", [item(1, "Skyrill migration starts Saturday", 0),
                                      item(2, "Pim leads the Skyrill migration", 1)], live=True)
    for _ in range(40):
        if pipe.stats()["packets"]:
            break
        import asyncio
        await asyncio.sleep(0.1)
    await pipe.stop()
    assert pipe.stats()["packets"] == 2


async def test_packet_sources_route_returns_original_messages(pipe):
    import httpx
    from fastapi import FastAPI

    from mini_cortex.pipeline_routes import make_router

    it = item(1, "Adaptive GPS beta gives seven days of battery", 0) | {"url": "https://slack.example/p1"}
    pipe.ingest("slack", "channels", [it], live=False)
    await pipe.process()
    pid = pipe.packets.all()[0]["id"]
    app = FastAPI()
    app.include_router(make_router(pipe))
    api = httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://ui")
    body = (await api.get(f"/api/packets/{pid}/sources")).json()
    assert body["packet"]["id"] == pid
    [s] = body["sources"]
    assert (s["text"], s["url"], s["author"], s["label"]) == (it["content"], "https://slack.example/p1", "pim", "#firmware")
    assert (await api.get("/api/packets/nope/sources")).status_code == 404

    one = (await api.get("/api/raw", params={"connector": "slack", "source_id": it["source_id"]})).json()
    assert (one["text"], one["url"], one["status"]) == (it["content"], "https://slack.example/p1", "packetized")
    assert (await api.get("/api/raw", params={"connector": "slack", "source_id": "nope"})).status_code == 404


def test_runaway_merge_is_contained():
    from pydantic import ValidationError

    from mini_cortex.dedup import Verdict, sane_merge
    from mini_cortex.packetize import clip

    loop = "and decision includes everything " * 200
    with pytest.raises(ValidationError):                    # the schema itself refuses a runaway verdict
        Verdict(action="MERGE", title=loop, content=loop)
    target = {"title": "Night mode cancelled", "content": "Night mode was cancelled on 28 Sep."}
    # Servers without guided decoding: checked again before writing; bad content means no merge at all.
    assert sane_merge(Verdict.model_construct(action="MERGE", title=loop, content=loop), target) is None
    assert sane_merge(Verdict.model_construct(action="MERGE", title=loop, content="Off until fw 4.3."), target) == \
        ("Night mode cancelled", "Off until fw 4.3.")
    assert len(clip(loop, 100)) <= 101 and clip("short", 100) == "short"


async def test_invalid_verdict_adds_instead_of_merging(pipe, monkeypatch):
    from pydantic import ValidationError

    pipe.ingest("slack", "channels", [item(1, "Adaptive GPS beta gives seven days of battery", 0)], live=False)
    await pipe.process()
    real = pipe.llm.extract

    async def broken(messages, schema, **kw):
        if schema.__name__ == "Verdict":
            return schema.model_validate({"action": "MERGE", "content": "x" * 5000})   # raises ValidationError
        return await real(messages, schema, **kw)

    monkeypatch.setattr(pipe.llm, "extract", broken)
    pipe.ingest("slack", "channels", [item(2, "Adaptive GPS beta gives seven days of battery update", 9)], live=False)
    await pipe.process()
    last = next(e for e in pipe.state.memory_log() if e["stage"] == "dedup")
    assert last["action"] == "ADD" and "invalid" in last["reason"]
    assert len(pipe.packets.all()) == 2
    assert ValidationError


async def test_type_is_a_preference_not_a_filter(pipe):
    from mini_cortex.search import hybrid_search

    pipe.ingest("slack", "channels", [item(1, "Night economy mode was cancelled for all collars", 0)], live=False)
    await pipe.process()                                   # the fake extractor stores it as a fact
    hits = await hybrid_search(pipe.llm, pipe.packets, "night economy mode", type_="preference")
    assert hits and "Night economy" in hits[0]["content"]


def test_rrf_prefers_items_in_both_lists():
    a, b, c = {"id": "a"}, {"id": "b"}, {"id": "c"}
    assert [x["id"] for x in rrf([a, b], [b, c])] == ["b", "a", "c"]


async def test_chat_searches_cites_and_streams(pipe):
    pipe.ingest("slack", "channels", [item(1, "Night economy mode was cancelled for all collars", 0)], live=False)
    await pipe.process()
    llm = FakeLLM(chat_script=[[("search_knowledge", '{"query": "night economy mode"}')],
                               ["Night mode is ", "off [1]."]])
    events = [e async for e in Chat(llm, pipe.packets, pipe.state, pipe.cfg).answer(None, "Is night mode on?")]
    kinds = [e["type"] for e in events]
    assert kinds == ["thread", "step", "token", "token", "citations", "done"]
    assert events[-2]["citations"][0]["title"].startswith("Night economy")
    tool_msg = llm.calls[1]["messages"][-1]
    assert tool_msg["role"] == "tool" and "untrusted_knowledge_data" in tool_msg["content"]
    thread = events[0]["thread_id"]
    assert [m["role"] for m in pipe.state.messages(thread)] == ["user", "assistant"]
    assert pipe.packets.all()[0]["access_count"] == 1


def test_grouped_citations_count():
    """Models sometimes write [3, 5] although the prompt asks for [3][5]: both cite 3 and 5."""
    assert cited_numbers("Phone only [3, 5], via Anni [1][4]; see [2,6]. Not a citation: [x], [ 7 ].") == {1, 2, 3, 4, 5, 6}
