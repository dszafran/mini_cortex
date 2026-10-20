"""End to end: a real SDK connector (from ../connectors) dials into the hub; the UI API sees frames, intake and
connector state."""

import asyncio
import inspect
import sys
from datetime import UTC, datetime, timedelta

import httpx

from mini_cortex import hub_base
from mini_cortex.hub import Hub
from mini_cortex.llm_config import REPO_ROOT, LLMSettings
from mini_cortex.pipeline import Pipeline
from mini_cortex.server import make_app

sys.path.insert(0, str(REPO_ROOT / "connectors"))
import connector_sdk  # noqa: E402
import dev_hub  # noqa: E402
from connector_sdk import Connector, Page, Stream, iso  # noqa: E402

T0 = datetime(2026, 10, 14, 6, 14, tzinfo=UTC)


def item(n: int, channel: str = "incidents") -> dict:
    return {"source_id": f"channels:C1:{n}", "timestamp": iso(T0 + timedelta(minutes=n)),
            "content": f"message {n}", "author": {"name": "Lachlan Reid"}, "thread_id": channel,
            "metadata": {"channel": channel}}


async def test_hub_records_protocol_and_serves_api(unused_tcp_port):
    hub = Hub("dev-token", backfill_days=None, store_path=None)
    hub_url = f"ws://127.0.0.1:{unused_tcp_port}/connectors"

    async def fetch(since, until, cursor, limit):
        return Page([item(1), item(2, "network")])

    conn = Connector("slack", hub_url, "dev-token", display_name="Slack")
    conn.add_stream(Stream("channels", kind="chat", fetch=fetch))

    async with hub.serving("127.0.0.1", unused_tcp_port):
        task = asyncio.create_task(conn.run())
        await asyncio.wait_for(conn.connected.wait(), 5)
        await conn.push("channels", [item(3)])

        api = httpx.AsyncClient(transport=httpx.ASGITransport(make_app(hub, hub_url, None)), base_url="http://ui")
        r = await api.post("/api/fetch", json={"connector": "slack", "stream": "channels",
                                               "since": iso(T0), "until": iso(T0 + timedelta(days=1))})
        assert r.json() == {"items": 2}

        conns = (await api.get("/api/connectors")).json()["connectors"]
        assert conns[0]["name"] == "slack" and conns[0]["status"] == "live"
        stream = conns[0]["streams"][0]
        assert stream["supports"] == ["fetch", "push"] and stream["items"] == 3 and stream["state"] == "done"
        assert {c["name"]: c["items"] for c in stream["channels"]} == {"incidents": 2, "network": 1}

        methods = [(f["dir"], f["method"]) for f in (await api.get("/api/frames")).json()]
        assert methods[:2] == [("in", "hello"), ("out", "welcome")]
        assert ("in", "push") in methods and ("out", "ack") in methods
        assert ("out", "fetch") in methods and ("in", "result") in methods

        log = (await api.get("/api/memory-log")).json()
        assert [e["action"] for e in log] == ["FETCH", "PUSH"]
        assert log[1]["reason"] == "Lachlan Reid: message 3"
        assert log[0]["detail"]["preview"][0] == {"author": "Lachlan Reid", "channel": "incidents",
                                                  "timestamp": iso(T0 + timedelta(minutes=1)), "text": "message 1"}

        missing = await api.post("/api/fetch", json={"connector": "nope", "stream": "x",
                                                     "since": iso(T0), "until": iso(T0)})
        assert missing.status_code == 404

        task.cancel()
        await asyncio.sleep(0.1)
        assert (await api.get("/api/connectors")).json()["connectors"][0]["status"] == "disconnected"


async def test_hub_feeds_the_pipeline(unused_tcp_port, tmp_path):
    """Fetched items wait as pending (Process button); pushed items also start the live buffer."""
    pipeline = Pipeline(LLMSettings(provider="custom", base_url="http://x/v1"), data=tmp_path)
    hub = Hub("dev-token", backfill_days=None, store_path=None, pipeline=pipeline)
    hub_url = f"ws://127.0.0.1:{unused_tcp_port}/connectors"

    async def fetch(since, until, cursor, limit):
        return Page([item(1), item(2, "network")])

    conn = Connector("slack", hub_url, "dev-token", display_name="Slack")
    conn.add_stream(Stream("channels", kind="chat", fetch=fetch))

    async with hub.serving("127.0.0.1", unused_tcp_port):
        task = asyncio.create_task(conn.run())
        await asyncio.wait_for(conn.connected.wait(), 5)
        api = httpx.AsyncClient(transport=httpx.ASGITransport(make_app(hub, hub_url, None)), base_url="http://ui")
        await api.post("/api/fetch", json={"connector": "slack", "stream": "channels",
                                           "since": iso(T0), "until": iso(T0 + timedelta(days=1))})
        assert pipeline.buffer == {}
        await conn.push("channels", [item(3)])
        await asyncio.sleep(0.2)
        assert list(pipeline.buffer) == ["slack:incidents"]

        stats = (await api.get("/api/stats")).json()
        assert (stats["intake"], stats["pending"], stats["packets"]) == (3, 3, 0)
        assert [e["action"] for e in (await api.get("/api/memory-log")).json()] == ["PUSH", "FETCH"]
        task.cancel()


def test_vendored_hub_matches_the_connectors():
    """hub_base.py is a copy of connectors/dev_hub.py: fail loudly when the protocol moves on without it."""
    assert hub_base.PROTOCOL_VERSION == connector_sdk.PROTOCOL_VERSION
    assert hub_base.LIMITS == dev_hub.LIMITS == connector_sdk.DEFAULT_LIMITS
    assert hub_base.REQUIRED_ITEM_FIELDS == dev_hub.REQUIRED_ITEM_FIELDS
    assert inspect.getsource(hub_base.DevHub) == inspect.getsource(dev_hub.DevHub), \
        "connectors/dev_hub.py changed: copy DevHub into app/mini_cortex/hub_base.py"
