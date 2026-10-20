"""HTTP routes behind Tune → Models. The key goes in (PUT) but never comes back out; GET shows only a hint.

    GET  /api/llm/settings   current settings (masked key), presets, which values the environment overrides
    PUT  /api/llm/settings   save to .env           body: SettingsUpdate (api_key omitted = keep the stored one)
    POST /api/llm/models     list the provider's models for unsaved settings (so the dropdowns fill before Save)
    POST /api/llm/test       run the doctor checks against unsaved settings
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path

from fastapi import APIRouter, HTTPException

from .doctor import explain, run_checks, short
from .llm import LLM
from .llm_config import (PRESETS, LLMSettings, SettingsUpdate, draft_settings, env_path, key_follows, key_hint,
                         raw_values, resolve, save_settings)

EMBED_HINTS = ("embed", "bge", "e5-", "gte-", "nomic")


def make_router(path: Path | None = None, on_saved: Callable[[LLMSettings], None] | None = None) -> APIRouter:
    """`on_saved` gets the new settings after a save, so running components can switch models without a restart."""
    router = APIRouter(prefix="/api/llm")
    file = lambda: path or env_path()  # noqa: E731 — resolved per request so MINI_CORTEX_ENV changes apply

    def draft(u: SettingsUpdate) -> LLMSettings:
        s = draft_settings(u, file())
        return s if key_follows(u, file()) else s.model_copy(update={"api_key": ""})

    def view() -> dict:
        values, overridden = raw_values(file())
        s = resolve(values)
        return {
            "provider": s.provider, "base_url": s.base_url, "project_id": s.project_id,
            "key_set": bool(s.api_key), "key_hint": key_hint(s.api_key),
            "chat_model": s.chat_model, "extract_model": s.extract_model,
            "embed_model": s.embed_model, "embed_dim": s.embed_dim, "ca_bundle": s.ca_bundle,
            "missing": s.missing(), "env_file": str(file()), "overridden": overridden,
            "presets": [p.model_dump() for p in PRESETS.values()],
        }

    @router.get("/settings")
    async def get_settings() -> dict:
        return view()

    @router.put("/settings")
    async def put_settings(u: SettingsUpdate) -> dict:
        if u.provider not in PRESETS:
            raise HTTPException(422, f"unknown provider {u.provider!r}")
        if u.provider == "scaleway" and any(c in u.project_id for c in "/:"):
            raise HTTPException(422, "The project ID looks like a URL. For another endpoint, pick 'Other / internal'.")
        s = draft(u)
        if s.preset.needs_key and not s.api_key:
            raise HTTPException(422, "New endpoint: enter its API key (the saved key stays with the old one).")
        save_settings(u, file())
        if on_saved:
            on_saved(resolve(raw_values(file())[0]))
        return view()

    @router.post("/models")
    async def models(u: SettingsUpdate) -> dict:
        s = draft(u)
        if not s.base_url or (s.preset.needs_key and not s.api_key):
            raise HTTPException(400, "Enter the API key first.")
        try:
            ids = await LLM(s, timeout=20, max_retries=0).list_models()
        except Exception as e:  # noqa: BLE001
            raise HTTPException(502, {"error": short(e), "fix": explain(e, s)}) from e
        return {"models": [{"id": m, "kind": "embed" if any(h in m.lower() for h in EMBED_HINTS) else "chat"}
                           for m in ids]}

    @router.post("/test")
    async def test(u: SettingsUpdate) -> dict:
        checks = await run_checks(draft(u))
        return {"checks": [asdict(c) for c in checks], "ok": all(c.status != "fail" for c in checks)}

    return router
