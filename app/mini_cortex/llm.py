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

from .llm_config import LLMSettings

log = logging.getLogger("llm")

# Asymmetric retrieval prefixes, as in Cortex. Titles are not embedded.
DOC_PREFIX = "search_document: "
QUERY_PREFIX = "search_query: "

FENCE = re.compile(r"^\s*```(?:json)?\s*(.*?)\s*```\s*$", re.S)


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


class LLM:
    def __init__(self, settings: LLMSettings, timeout: float = 60, max_retries: int = 2,
                 deadline: float | None = None, stall_retries: int = 2):
        """`timeout` is httpx's: it limits the wait between bytes, so a gateway that keeps a request open without
        ever answering is never cut off. `deadline` limits a whole call; a stalled call is retried `stall_retries`
        times, then raises Stalled."""
        self.s = settings
        self.deadline, self.stall_retries = deadline, stall_retries
        # Local servers accept any key, but the SDK refuses an empty one.
        key = settings.api_key or "not-needed"
        http = httpx.AsyncClient(verify=ssl_context(settings.ca_bundle), timeout=timeout)
        self.client = AsyncOpenAI(base_url=settings.base_url, api_key=key, max_retries=max_retries, http_client=http)
        self.embed_client = (AsyncOpenAI(base_url=settings.embed_base_url, api_key=key, max_retries=max_retries,
                                         http_client=http) if settings.embed_base_url else self.client)
        self.native_json: bool | None = None   # learnt on first extract: does the server do json_schema?
        self.send_dimensions = True            # dropped if the embedding server rejects `dimensions`

    async def _call[R](self, make: Callable[[], Awaitable[R]]) -> R:
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
        return sorted([m.id async for m in self.client.models.list()])

    # --- chat -------------------------------------------------------------------------------------------

    async def chat(self, messages: list[dict], *, model: str | None = None, tools: list[dict] | None = None,
                   **kwargs: Any) -> Any:
        """One chat completion; returns the message (`.content`, `.tool_calls`)."""
        if tools:
            kwargs["tools"] = tools
        r = await self._call(lambda: self.client.chat.completions.create(model=model or self.s.chat_model,
                                                                         messages=messages, **kwargs))
        return r.choices[0].message

    async def stream(self, messages: list[dict], *, model: str | None = None, **kwargs: Any) -> AsyncIterator[str]:
        """Text deltas as they arrive."""
        s = await self.client.chat.completions.create(model=model or self.s.chat_model, messages=messages,
                                                      stream=True, **kwargs)
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
        r = await self._call(lambda: self.client.chat.completions.create(
            model=model or self.s.extract_model, messages=messages, response_format=rf, temperature=0, **kwargs))
        return schema.model_validate_json(strip_fences(r.choices[0].message.content))

    async def extract_prompted[T: BaseModel](self, messages: list[dict], schema: type[T], *,
                                             model: str | None = None, attempts: int = 2, **kwargs: Any) -> T:
        instruction = ("Reply with a single JSON object and nothing else. It must match this JSON Schema:\n"
                       + json.dumps(schema.model_json_schema()))
        msgs = [*messages, {"role": "system", "content": instruction}]
        for attempt in range(attempts):
            r = await self._call(lambda: self.client.chat.completions.create(
                model=model or self.s.extract_model, messages=msgs, temperature=0, **kwargs))
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
            r = await self._call(lambda: self.embed_client.embeddings.create(model=self.s.embed_model, input=inputs,
                                                                             **kwargs))
        except openai.BadRequestError as e:
            if "dimensions" not in kwargs or "dimension" not in str(e).lower():
                raise
            log.info("embedding server rejects `dimensions`; using the model's native size")
            self.send_dimensions = False
            r = await self._call(lambda: self.embed_client.embeddings.create(model=self.s.embed_model, input=inputs))
        return [d.embedding for d in sorted(r.data, key=lambda d: d.index)]
