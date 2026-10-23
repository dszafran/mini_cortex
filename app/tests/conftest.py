"""A stub OpenAI-compatible server, so the LLM layer is tested end to end over real HTTP without a key."""

import asyncio
import json
import re
import socket
import threading
import time
from pathlib import Path

import pytest
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

GOOD_KEY = "sk-test-good-key-1234"
WARSAW = {"city": "Warsaw", "population_millions": 1.8, "landmarks": ["Palace of Culture", "Old Town", "Łazienki"]}


class Upstream:
    def __init__(self):
        self.no_json_schema = False   # behave like a stack without guided decoding
        self.ignore_dims = False      # behave like a server that ignores `dimensions`
        self.no_tools = False         # model answers in text instead of calling the tool
        self.stall = 0                # leave this many chat requests unanswered (a flaky gateway)
        self.no_credit = False        # answer 402, like OpenRouter when the key's credit is used up
        self.rate_limited = 0         # answer this many POSTs with 429, like Scaleway's per-minute quota
        self.requests: list[dict] = []


def stub_app(up: Upstream) -> FastAPI:
    app = FastAPI()

    @app.middleware("http")
    async def auth(request: Request, call_next):
        if request.headers.get("authorization") != f"Bearer {GOOD_KEY}":
            return JSONResponse({"error": {"message": "Invalid API key", "type": "invalid_request_error"}}, 401)
        if up.no_credit and request.method == "POST":
            return JSONResponse({"error": {"message": "Insufficient credits", "code": 402}}, 402)
        if up.rate_limited > 0 and request.method == "POST":
            up.rate_limited -= 1
            return JSONResponse({"status": 429, "error": "INSUFFICIENT QUOTA",
                                 "message": "You exceeded your current quota of requests per minute."}, 429,
                                headers={"x-ratelimit-remaining-requests": "0", "x-ratelimit-reset-requests": "10ms"})
        return await call_next(request)

    @app.get("/v1/embeddings/models")       # OpenRouter lists its embedding models apart from /models
    async def embedding_models():
        return {"data": [{"id": "qwen/qwen3-embedding-8b"}]}

    @app.get("/v1/models")
    async def models():
        return {"object": "list", "data": [{"id": m, "object": "model", "created": 0, "owned_by": "x"}
                                            for m in ("glm-5.3", "deepseek-v4.1-flash", "qwen3-embedding-8b")]}

    def completion(message: dict) -> dict:
        return {"id": "c1", "object": "chat.completion", "created": 0, "model": "m",
                "choices": [{"index": 0, "message": {"role": "assistant", **message}, "finish_reason": "stop"}]}

    @app.post("/v1/chat/completions")
    async def chat(body: dict):
        up.requests.append(body)
        if up.stall > 0:
            up.stall -= 1
            await asyncio.sleep(5)
        if body["model"] not in ("glm-5.3", "deepseek-v4.1-flash"):
            return JSONResponse({"error": {"message": f"model {body['model']} not found"}}, 404)
        if body.get("tools") and not up.no_tools:
            return completion({"content": None, "tool_calls": [{"id": "t1", "type": "function", "function": {
                "name": "get_current_time", "arguments": json.dumps({"city": "Warsaw"})}}]})
        if body.get("response_format"):
            if up.no_json_schema:
                return JSONResponse({"error": {"message": "response_format json_schema is not supported"}}, 400)
            return completion({"content": json.dumps(WARSAW)})
        if any("JSON Schema" in (m.get("content") or "") for m in body["messages"]):
            return completion({"content": "```json\n" + json.dumps(WARSAW) + "\n```"})
        return completion({"content": "pong"})

    @app.post("/v1/embeddings")
    async def embeddings(body: dict):
        up.requests.append(body)
        n = 4096 if up.ignore_dims or "dimensions" not in body else body["dimensions"]
        inputs = body["input"] if isinstance(body["input"], list) else [body["input"]]
        return {"object": "list", "model": body["model"], "usage": {"prompt_tokens": 1, "total_tokens": 1},
                "data": [{"object": "embedding", "index": i, "embedding": [0.0] * n} for i in range(len(inputs))]}

    return app


@pytest.fixture(scope="session")
def server():
    up = Upstream()
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    srv = uvicorn.Server(uvicorn.Config(stub_app(up), host="127.0.0.1", port=port, log_level="warning"))
    threading.Thread(target=srv.run, daemon=True).start()
    while not srv.started:
        time.sleep(0.02)
    yield up, f"http://127.0.0.1:{port}/v1"
    srv.should_exit = True


@pytest.fixture
def upstream(server):
    up, url = server
    up.no_json_schema = up.ignore_dims = up.no_tools = up.no_credit = False
    up.stall = up.rate_limited = 0
    up.requests.clear()
    return up, url


@pytest.fixture
def env_file(tmp_path, monkeypatch):
    for k in ("LLM_PROVIDER", "LLM_BASE_URL", "LLM_API_KEY", "SCW_SECRET_KEY", "SCW_PROJECT_ID",
              "CHAT_MODEL", "EXTRACT_MODEL", "EMBED_MODEL", "EMBED_DIM", "EMBED_BASE_URL"):
        monkeypatch.delenv(k, raising=False)
    path = tmp_path / ".env"
    monkeypatch.setenv("MINI_CORTEX_ENV", str(path))
    return path


# --- workshop step branches -----------------------------------------------------------------------------------------
# A step branch also carries the later steps' exercises as TODOs. Their tests would fail there for a reason that isn't
# this step's, so they're skipped with the step they belong to; this step's own tests still fail until it's solved.
# The current step comes from STEP.md (none on main: nothing is skipped).
LATER_STEPS = {           # test -> the latest workshop step whose exercise it needs
    "test_doctor_all_green": 0, "test_extract_native_and_fallback": 0,
    "test_a_failing_window_does_not_stop_the_run_and_is_retried": 2, "test_live_buffer_flushes_after_n_messages": 2,
    "test_packet_sources_route_returns_original_messages": 2, "test_type_is_a_preference_not_a_filter": 2,
    "test_unreachable_model_stops_the_run_and_keeps_items_pending": 2,
    "test_invalid_verdict_adds_instead_of_merging": 3, "test_process_adds_skips_and_merges": 3,
    "test_chat_searches_cites_and_streams": 4, "test_rrf_prefers_items_in_both_lists": 4,
}


def current_step() -> int | None:
    step_md = Path(__file__).resolve().parents[2] / "STEP.md"
    m = re.match(r"# Step (\d+)", step_md.read_text(encoding="utf-8")) if step_md.is_file() else None
    return int(m[1]) if m else None


def pytest_collection_modifyitems(config, items):
    now = current_step()
    if now is None:
        return
    for item in items:
        step = LATER_STEPS.get(item.name.split("[")[0])
        if step is not None and step > now:
            item.add_marker(pytest.mark.skip(reason=f"comes in workshop step {step}"))
