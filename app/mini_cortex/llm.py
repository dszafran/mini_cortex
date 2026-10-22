"""One OpenAI-compatible client for every model role: chat, extraction (structured output) and embeddings.

    llm = LLM(load_settings())
    reply = await llm.chat([{"role": "user", "content": "hi"}])
    packets = await llm.extract(messages, PacketList)          # validated Pydantic object
    vectors = await llm.embed(["text"], kind="document")

Scaleway, a vLLM deployment, Ollama and OpenAI all speak this API, so switching provider is a settings change.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import re
import ssl
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any, Literal

import certifi
import httpx
import openai
from openai import AsyncOpenAI
from pydantic import BaseModel, ValidationError

from .llm_config import PRESETS, LLMSettings

log = logging.getLogger("llm")

# Asymmetric retrieval prefixes, as in Cortex. Titles are not embedded.
DOC_PREFIX = "search_document: "
QUERY_PREFIX = "search_query: "

FENCE = re.compile(r"^\s*```(?:json)?\s*(.*?)\s*```\s*$", re.S)

# OpenRouter sends each request to one of several providers. Chat and extraction only go to providers that support
# every parameter sent (tools, json_schema): one of the seven qwen3-235b endpoints has neither. Embeddings go first to
# the providers whose qwen3-embedding-8b vectors matched Scaleway's (cosine 0.9999; DeepInfra, the fallback, 0.996).
OPENROUTER_CHAT = {"extra_body": {"provider": {"require_parameters": True}}}
OPENROUTER_EMBED = {"extra_body": {"provider": {"order": ["nebius", "siliconflow"]}}}


DURATION = re.compile(r"^(?:(\d+)m(?!s))?(?:(\d+(?:\.\d+)?)s)?(?:(\d+)ms)?$")   # "59s985ms", "1m2s", "100ms"


def reset_seconds(e: openai.APIStatusError) -> float | None:
    """When the provider says the limit resets: `retry-after` or Scaleway's `x-ratelimit-reset-*`. None if unsaid."""
    h, out = e.response.headers, []
    try:
        out.append(float(h["retry-after"]))
    except (KeyError, ValueError):
        pass
    for name in ("x-ratelimit-reset-requests", "x-ratelimit-reset-tokens"):
        if (m := DURATION.match(h.get(name, "").strip())) and any(m.groups()):
            out.append(int(m[1] or 0) * 60 + float(m[2] or 0) + int(m[3] or 0) / 1000)
    return max(out) if out else None


def strip_fences(text: str) -> str:
    m = FENCE.match(text or "")
    return m[1] if m else (text or "").strip()


def ssl_context(ca_bundle: str = "") -> ssl.SSLContext:
    """The usual public roots, plus an optional extra root CA (company proxy, internal inference endpoint)."""
    ctx = ssl.create_default_context(cafile=certifi.where())
    if ca_bundle:
        ctx.load_verify_locations(cafile=str(Path(ca_bundle).expanduser()))
    return ctx


class Stalled(TimeoutError):
    """No complete answer within the deadline, on every try."""


class OutOfCredits(RuntimeError):
    """HTTP 402: the key has no credit left (OpenRouter), so every other call would fail the same way."""


class EmbeddingSize(RuntimeError):
    """The server ignores `dimensions` and the model can't be cut safely: stop with the size to set, don't store
    vectors of the wrong length or meaning."""


def model_name(model: str) -> str:
    """`qwen3-embedding-8b`, `qwen/qwen3-embedding-8b` and `generative-apis/qwen3-embedding-8b` are one model."""
    return model.rsplit("/", 1)[-1].lower()


def cuttable(model: str) -> bool:
    """Only the presets' own embedding models were measured as Matryoshka (cut vectors = what a server that honours
    `dimensions` returns). Cutting any other model (e.g. bge-multilingual-gemma2) would keep its size but lose its
    meaning, so that one has to run at its native size instead."""
    return any(p.embed_model and model_name(p.embed_model) == model_name(model) for p in PRESETS.values())


def cut(vector: list[float], n: int) -> list[float]:
    """A Matryoshka embedding at a smaller size: the first n values, renormalized. This is what a server that honours
    `dimensions` returns (measured: cosine 0.9999 against Scaleway's own 1024-dim vectors)."""
    head = vector[:n]
    length = math.sqrt(sum(x * x for x in head)) or 1.0
    return [x / length for x in head]


class LLM:
    def __init__(self, settings: LLMSettings, timeout: float = 60, max_retries: int = 2,
                 deadline: float | None = None, stall_retries: int = 2, rate_limit_wait: float = 0):
        """`timeout` is httpx's: it limits the wait between bytes, so a gateway that keeps a request open without
        ever answering is never cut off. `deadline` limits a whole call; a stalled call is retried `stall_retries`
        times, then raises Stalled. `rate_limit_wait`: how long a call may keep waiting out 429s (the provider's
        per-minute or concurrency limit) before it gives up; 0 = fail at once, as the doctor wants."""
        self.s = settings
        self.deadline, self.stall_retries, self.rate_limit_wait = deadline, stall_retries, rate_limit_wait
        self.rate_limited = 0                  # calls that had to wait for a 429, for the logs and stats
        # Local servers accept any key, but the SDK refuses an empty one.
        key = settings.api_key or "not-needed"
        self.http = httpx.AsyncClient(verify=ssl_context(settings.ca_bundle), timeout=timeout)
        self.client = AsyncOpenAI(base_url=settings.base_url, api_key=key, max_retries=max_retries,
                                  http_client=self.http)
        self.embed_client = (AsyncOpenAI(base_url=settings.embed_base_url, api_key=key, max_retries=max_retries,
                                         http_client=self.http) if settings.embed_base_url else self.client)
        self.native_json: bool | None = None   # learnt on first extract: does the server do json_schema?
        self.send_dimensions = True            # dropped if the embedding server rejects `dimensions`
        self.cut_from: int | None = None       # the size the server returned when it ignored `dimensions`
        openrouter = settings.provider == "openrouter"
        self.chat_extra = OPENROUTER_CHAT if openrouter else {}
        self.embed_extra = OPENROUTER_EMBED if openrouter and not settings.embed_base_url else {}

    async def call[R](self, make: Callable[[], Awaitable[R]], max_wait: float | None = None) -> R:
        """One API call with the deadline, and waiting out rate limits. The SDK's own retries give up within seconds,
        but Scaleway's quota is shared by the whole organisation (all keys, the whole room): back off and try again, up
        to `max_wait` seconds in total (default: `rate_limit_wait`). The quota refills continuously (measured: a call
        right after a refused burst succeeds), so short backoffs beat its "reset in 60 s" header; that header only
        shortens a backoff, never stretches it."""
        budget = self.rate_limit_wait if max_wait is None else max_wait
        waited, backoff = 0.0, 2.0
        while True:
            try:
                return await self._within_deadline(make)
            except openai.RateLimitError as e:
                # Jitter, so a room full of laptops that hit the limit together doesn't retry in lockstep.
                pause = min(reset_seconds(e) or backoff, backoff) + random.uniform(0.5, 3)
                if waited + pause > budget:
                    raise
                self.rate_limited += 1
                log.warning("rate limited (%s); waiting %.0f s, then trying again", e.message[:80], pause)
                await asyncio.sleep(pause)
                waited, backoff = waited + pause, min(backoff * 2, 30)
            except openai.APIStatusError as e:
                if e.status_code == 402:
                    raise OutOfCredits(e.message) from e
                raise

    async def _within_deadline[R](self, make: Callable[[], Awaitable[R]]) -> R:
        if not self.deadline:
            return await make()
        for attempt in range(1, self.stall_retries + 2):
            try:
                return await asyncio.wait_for(make(), self.deadline)
            except TimeoutError:
                log.warning("no answer within %.0f s (try %d of %d)", self.deadline, attempt, self.stall_retries + 1)
        raise Stalled(f"no answer within {self.deadline:.0f} s, {self.stall_retries + 1} tries")

    # --- models -----------------------------------------------------------------------------------------

    async def list_models(self) -> list[str]:
        ids = [m.id async for m in self.client.models.list()]
        if self.s.provider == "openrouter":            # OpenRouter lists its embedding models apart from the others
            r = await self.http.get(f"{self.s.base_url}/embeddings/models",
                                    headers={"Authorization": f"Bearer {self.client.api_key}"})
            r.raise_for_status()
            ids += [m["id"] for m in r.json().get("data", [])]
        return sorted(set(ids))

    # --- chat -------------------------------------------------------------------------------------------

    async def chat(self, messages: list[dict], *, model: str | None = None, tools: list[dict] | None = None,
                   **kwargs: Any) -> Any:
        """One chat completion; returns the message (`.content`, `.tool_calls`)."""
        if tools:
            kwargs["tools"] = tools
        r = await self.call(lambda: self.client.chat.completions.create(model=model or self.s.chat_model,
                                                                         messages=messages, **self.chat_extra,
                                                                         **kwargs))
        return r.choices[0].message

    async def stream(self, messages: list[dict], *, model: str | None = None, **kwargs: Any) -> AsyncIterator[str]:
        """Text deltas as they arrive."""
        s = await self.client.chat.completions.create(model=model or self.s.chat_model, messages=messages,
                                                      stream=True, **self.chat_extra, **kwargs)
        async for chunk in s:
            if chunk.choices and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content

    # --- structured output ------------------------------------------------------------------------------

    async def extract[T: BaseModel](self, messages: list[dict], schema: type[T], *, model: str | None = None,
                                    **kwargs: Any) -> T:
        """Ask for JSON that matches `schema`. Uses `response_format: json_schema` where the server supports it
        (guided decoding: the server enforces the schema, including maxLength, while the model writes), otherwise
        puts the schema into the prompt and validates the reply. `kwargs` go to the API call (e.g. max_tokens)."""
        if self.native_json is not False:
            try:
                out = await self.extract_native(messages, schema, model=model, **kwargs)
                self.native_json = True
                return out
            except openai.BadRequestError as e:
                if self.native_json:
                    raise
                log.info("json_schema not supported here (%s); using prompt + parse", e.message)
                self.native_json = False
        return await self.extract_prompted(messages, schema, model=model, **kwargs)

    async def extract_native[T: BaseModel](self, messages: list[dict], schema: type[T], *,
                                           model: str | None = None, **kwargs: Any) -> T:
        rf = {"type": "json_schema", "json_schema": {"name": schema.__name__, "schema": schema.model_json_schema()}}
        r = await self.call(lambda: self.client.chat.completions.create(
            model=model or self.s.extract_model, messages=messages, response_format=rf, temperature=0,
            **self.chat_extra, **kwargs))
        return schema.model_validate_json(strip_fences(r.choices[0].message.content))

    async def extract_prompted[T: BaseModel](self, messages: list[dict], schema: type[T], *,
                                             model: str | None = None, attempts: int = 2, **kwargs: Any) -> T:
        instruction = ("Reply with a single JSON object and nothing else. It must match this JSON Schema:\n"
                       + json.dumps(schema.model_json_schema()))
        msgs = [*messages, {"role": "system", "content": instruction}]
        for attempt in range(attempts):
            r = await self.call(lambda: self.client.chat.completions.create(
                model=model or self.s.extract_model, messages=msgs, temperature=0, **self.chat_extra, **kwargs))
            text = r.choices[0].message.content or ""
            try:
                return schema.model_validate_json(strip_fences(text))
            except ValidationError as e:
                if attempt == attempts - 1:
                    raise
                msgs = [*msgs, {"role": "assistant", "content": text},
                        {"role": "user", "content": f"That JSON was invalid: {e.errors()[:3]}. Send the corrected JSON only."}]
        raise AssertionError("unreachable")

    # --- embeddings -------------------------------------------------------------------------------------

    async def embed(self, texts: list[str], *, kind: Literal["document", "query", "raw"] = "document") -> list[list[float]]:
        prefix = {"document": DOC_PREFIX, "query": QUERY_PREFIX, "raw": ""}[kind]
        inputs = [prefix + t for t in texts]
        kwargs: dict[str, Any] = {}
        if self.s.embed_dim and self.send_dimensions:
            kwargs["dimensions"] = self.s.embed_dim
        try:
            r = await self.call(lambda: self.embed_client.embeddings.create(model=self.s.embed_model, input=inputs,
                                                                             **self.embed_extra, **kwargs))
        except openai.BadRequestError as e:
            if "dimensions" not in kwargs or "dimension" not in str(e).lower():
                raise
            log.info("embedding server rejects `dimensions`; using the model's native size")
            self.send_dimensions = False
            r = await self.call(lambda: self.embed_client.embeddings.create(model=self.s.embed_model, input=inputs,
                                                                             **self.embed_extra))
        vectors = [d.embedding for d in sorted(r.data, key=lambda d: d.index)]
        if self.s.embed_dim and vectors and len(vectors[0]) > self.s.embed_dim:
            if not cuttable(self.s.embed_model):
                raise EmbeddingSize(f"{self.s.embed_model} returned {len(vectors[0])} dims and ignores `dimensions`. "
                                    f"Set the embedding size to {len(vectors[0])} (Tune → Models), then Rebuild memory.")
            # The server ignored `dimensions`: cut here instead, so changing provider never breaks a memory.
            self.cut_from = len(vectors[0])
            vectors = [cut(v, self.s.embed_dim) for v in vectors]
        return vectors
