"""`uv run doctor`: is the model endpoint usable for mini-cortex?

Checks the settings, lists the provider's models, then runs one real call per capability the app needs:
chat, structured output (packetize/dedup), tool calling (the chat agent) and embeddings (with the configured size).
Each failure comes with a fix. The UI's "Test connection" button runs the same checks.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

import openai
from pydantic import BaseModel

from .llm import LLM, EmbeddingSize, OutOfCredits
from .llm_config import LLMSettings, env_path, load_settings

Status = Literal["ok", "warn", "fail", "skip"]


@dataclass
class Check:
    name: str
    status: Status
    detail: str
    fix: str = ""
    ms: int | None = None
    fatal: bool = False   # wrong key / URL / network: every other call would fail the same way


class Probe(BaseModel):
    city: str
    population_millions: float
    landmarks: list[str]


TIME_TOOL = {"type": "function", "function": {
    "name": "get_current_time",
    "description": "Current local time in a city.",
    "parameters": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]}}}


FATAL = (openai.AuthenticationError, openai.PermissionDeniedError, openai.APIConnectionError, OutOfCredits)


def explain(e: Exception, s: LLMSettings, model: str = "") -> str:
    """A fix for the errors people actually hit."""
    if isinstance(e, OutOfCredits):
        return ("No credit left on this key. Ask your instructor for another one, or switch provider in "
                "Tune → Models.")
    if isinstance(e, openai.AuthenticationError):
        if model and "model" in str(e).lower():     # gateways answer an unknown model with 401, not 404
            return f"Model {model!r} is not available for this key. Pick one from the model list."
        where = s.preset.key_help if s.provider == "scaleway" else "Check that it is complete and belongs to this provider."
        return f"The key was rejected. {where}"
    if isinstance(e, openai.PermissionDeniedError):
        # Scaleway answers an expired or revoked key with 403 too, the same as a missing policy.
        if s.provider == "scaleway":
            return ("Scaleway refused the key: it may have expired or been revoked, or its policy lacks Generative APIs "
                    "access in this project. Create a new key in Console → IAM → API keys.")
        return "The key was refused here: it may have expired or been revoked, or it lacks access to this endpoint."
    if isinstance(e, openai.NotFoundError):
        if model:
            return f"Model {model!r} is not served here. Pick one from the model list."
        return "Nothing at this URL. Check the base URL (it must end in /v1) and the project id."
    if isinstance(e, openai.RateLimitError):
        return "Rate limited or out of quota. Wait a minute, or check the quota in the provider console."
    if isinstance(e, openai.APITimeoutError):
        return ("No answer in time, even after retries. If the other checks are fast, the endpoint is dropping "
                "requests: measure it with `uv run python scripts/probe_chat.py`. Otherwise pick a smaller model.")
    if isinstance(e, openai.APIConnectionError):
        cause = repr(e.__cause__ or e)
        if "CERTIFICATE" in cause.upper() or "SSL" in cause.upper():
            return ("TLS failed: the server's certificate comes from a root this machine doesn't trust (company CA or "
                    "proxy). Export that root as PEM and set it as the extra CA certificate (LLM_CA_BUNDLE). macOS: "
                    "security find-certificate -c \"<root name>\" -p > ~/company-ca.pem")
        return f"Cannot reach {s.base_url}. Check the URL, Wi-Fi and any proxy (HTTPS_PROXY)."
    if isinstance(e, openai.BadRequestError):
        return f"The server refused the request: {e.message}"
    return ""


def short(e: Exception) -> str:
    msg = getattr(e, "message", None) or str(e) or type(e).__name__
    if isinstance(e, openai.APIStatusError) and isinstance(e.body, dict):
        body = e.body.get("error", e.body)
        inner = body.get("message") if isinstance(body, dict) else None
        msg = f"HTTP {e.status_code}: {inner}" if inner else msg
    return " ".join(msg.split())[:240]


STALLED = ("The endpoint sometimes leaves a request unanswered. The app retries timed-out calls, so it works, "
           "but expect slow spots during ingest.")


async def timed(name: str, s: LLMSettings, model: str, coro_fn, retries: int = 2) -> Check:
    """Run one check. A timed-out call is retried (as the app does) and reported, so a flaky endpoint shows up as a
    warning, not as a broken one."""
    t0 = time.perf_counter()
    fatal = False
    stalls = 0
    while True:
        try:
            status, detail, fix = await coro_fn()
            if stalls and status == "ok":
                status, fix = "warn", STALLED
            if stalls:
                detail += f" · after {stalls} timed-out {'try' if stalls == 1 else 'tries'}"
            break
        except openai.APITimeoutError as e:
            stalls += 1
            if stalls <= retries:
                continue
            status, detail, fix = "fail", f"{short(e)} ({stalls} tries)", explain(e, s, model)
            break
        except Exception as e:  # noqa: BLE001 — every failure becomes a red line, not a traceback
            status, detail, fix = "fail", short(e), explain(e, s, model)
            fatal = isinstance(e, FATAL)
            break
    return Check(name, status, detail, fix, round((time.perf_counter() - t0) * 1000), fatal)


async def run_checks(s: LLMSettings, timeout: float = 30) -> list[Check]:
    missing = s.missing()
    if missing:
        return [Check("settings", "fail", f"missing: {', '.join(missing)}",
                      "Fill these in on Tune → Models, or in .env (see .env.example).")]
    llm = LLM(s, timeout=timeout, max_retries=0)
    checks = [Check("settings", "ok", f"{s.preset.label} · {s.base_url}")]

    async def models():
        ids = await llm.list_models()
        absent = [m for m in (s.chat_model, s.extract_model, s.embed_model) if ids and m not in ids]
        if absent:
            return "warn", f"{len(ids)} models; not listed: {', '.join(absent)}", "Pick models from the list."
        return "ok", f"{len(ids)} models available", ""

    checks.append(await timed("models", s, "", models))
    if checks[-1].fatal:
        return checks

    async def chat():
        msg = await llm.chat([{"role": "user", "content": "Reply with the single word: pong"}], max_tokens=200)
        text = (msg.content or "").strip()
        if not text:
            return "warn", "empty reply", "The model answered with no text; try another chat model."
        return "ok", f"{s.chat_model}: {text[:60]!r}", ""

    async def structured():
        q = [{"role": "user", "content": "Facts about Warsaw: city name, population in millions, three landmarks."}]
        try:
            p = await llm.extract_native(q, Probe)
            return "ok", f"{s.extract_model}: json_schema · {p.city}, {len(p.landmarks)} landmarks", ""
        except (openai.BadRequestError, ValueError) as native_err:
            p = await llm.extract_prompted(q, Probe)
            return ("warn", f"{s.extract_model}: json_schema refused ({short(native_err)[:80]}); prompt + parse works",
                    "Works, but slower and less reliable. A model/server with guided decoding is better for packetize.")

    async def tools():
        msg = await llm.chat([{"role": "user", "content": "What time is it in Warsaw right now? Use the tool."}],
                             tools=[TIME_TOOL], max_tokens=300)
        calls = msg.tool_calls or []
        if not calls:
            return ("fail", f"{s.chat_model} answered without calling the tool",
                    "The chat agent needs tool calling. Pick another chat model (vLLM: start it with a tool-call parser).")
        args = json.loads(calls[0].function.arguments or "{}")
        return "ok", f"{s.chat_model}: {calls[0].function.name}({json.dumps(args)})", ""

    async def embeddings():
        try:
            [vec] = await llm.embed(["mini-cortex doctor"], kind="raw")
        except EmbeddingSize as e:
            return "fail", f"{s.embed_model}: the server ignores `dimensions`", str(e)
        if llm.cut_from:
            return ("ok", f"{s.embed_model}: {len(vec)} dims, cut here from {llm.cut_from} (a Matryoshka model)", "")
        if s.embed_dim and len(vec) != s.embed_dim:
            return ("warn", f"{s.embed_model}: got {len(vec)} dims, asked for {s.embed_dim}",
                    f"The server ignores `dimensions`. Set the embedding size to {len(vec)}.")
        return "ok", f"{s.embed_model}: {len(vec)} dims", ""

    checks += await asyncio.gather(
        timed("chat", s, s.chat_model, chat),
        timed("structured output", s, s.extract_model, structured),
        timed("tool calling", s, s.chat_model, tools),
        timed("embeddings", s, s.embed_model, embeddings),
    )
    return checks


# --- CLI -------------------------------------------------------------------------------------------------

MARK = {"ok": ("\033[32m", "✔"), "warn": ("\033[33m", "!"), "fail": ("\033[31m", "✘"), "skip": ("\033[90m", "-")}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", type=Path, default=None, help=f"settings file (default {env_path()})")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    args = ap.parse_args()

    s = load_settings(args.env)
    checks = asyncio.run(run_checks(s))
    if args.json:
        print(json.dumps([asdict(c) for c in checks], indent=2))
    else:
        color = sys.stdout.isatty()
        print(f"mini-cortex doctor · settings from {args.env or env_path()}\n")
        for c in checks:
            tint, mark = MARK[c.status]
            head = f"{tint}{mark}\033[0m" if color else mark
            ms = f"  ({c.ms} ms)" if c.ms is not None else ""
            print(f" {head} {c.name:<18} {c.detail}{ms}")
            if c.fix and c.status != "ok":
                print(f"   {'':<18} → {c.fix}")
        bad = sum(c.status == "fail" for c in checks)
        warn = sum(c.status == "warn" for c in checks)
        summary = f"{bad} check(s) failed." if bad else "All green." if not warn else f"Usable, {warn} warning(s)."
        print("\n" + summary)
    sys.exit(1 if any(c.status == "fail" for c in checks) else 0)


if __name__ == "__main__":
    main()
