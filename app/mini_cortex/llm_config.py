"""LLM settings: which OpenAI-compatible endpoint to call, with which key, and which model for each role.

Everything lives in the repo-root `.env` (override the path with MINI_CORTEX_ENV). Real environment variables win
over the file, so the UI reports them as "overridden" instead of silently ignoring a save.

    LLM_PROVIDER    scaleway | ollama | custom
    SCW_SECRET_KEY  Scaleway API key       } the same variables OpenCode's Scaleway provider reads,
    SCW_PROJECT_ID  Scaleway project id    } so one key serves both
    LLM_BASE_URL    any OpenAI-compatible /v1 URL (ollama, custom)
    LLM_API_KEY     key for LLM_BASE_URL
    CHAT_MODEL      answers questions in chat (tool calling)
    EXTRACT_MODEL   packetize + dedup verdicts (structured output, high volume)
    EMBED_MODEL     embeddings
    EMBED_DIM       embedding size; Matryoshka models (qwen3-embedding-8b) honour `dimensions`
    EMBED_BASE_URL  optional: embeddings from a different endpoint (same key)
    LLM_CA_BUNDLE   optional: PEM file with an extra root CA (company proxy, internal endpoint), trusted on top of
                    the usual public roots
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import BaseModel

REPO_ROOT = Path(__file__).resolve().parents[2]
SCALEWAY_URL = "https://api.scaleway.ai/v1"


class Preset(BaseModel):
    id: str
    label: str
    base_url: str              # shown in the UI; for Scaleway it is derived from the project id
    needs_key: bool
    key_env: str
    key_help: str
    chat_model: str = ""
    extract_model: str = ""
    embed_model: str = ""
    embed_dim: int = 0


PRESETS: dict[str, Preset] = {
    "scaleway": Preset(
        id="scaleway", label="Scaleway Generative APIs", base_url="https://api.scaleway.ai/<project id>/v1",
        needs_key=True, key_env="SCW_SECRET_KEY",
        key_help="Scaleway console → IAM → API keys. The key's policy needs Generative APIs access in this project.",
        chat_model="qwen3-235b-a22b-instruct-2507", extract_model="qwen3-235b-a22b-instruct-2507",
        embed_model="qwen3-embedding-8b", embed_dim=1024),
    "ollama": Preset(
        id="ollama", label="Ollama (local)", base_url="http://localhost:11434/v1", needs_key=False, key_env="LLM_API_KEY",
        key_help="No key needed. Pull the models first, e.g. `ollama pull qwen3:8b`."),
    "custom": Preset(
        id="custom", label="Other / internal (OpenAI-compatible)", base_url="", needs_key=True, key_env="LLM_API_KEY",
        key_help="Any server that speaks the OpenAI /v1 API: an internal inference endpoint, vLLM, LM Studio, OpenAI, …"),
}


class LLMSettings(BaseModel):
    """Resolved settings, ready to build a client from."""
    provider: str = "scaleway"
    base_url: str = SCALEWAY_URL
    api_key: str = ""
    project_id: str = ""
    chat_model: str = ""
    extract_model: str = ""
    embed_model: str = ""
    embed_dim: int = 0
    embed_base_url: str = ""
    ca_bundle: str = ""

    @property
    def preset(self) -> Preset:
        return PRESETS.get(self.provider, PRESETS["custom"])

    def missing(self) -> list[str]:
        """What still has to be filled in before any call can work."""
        out = []
        if not self.base_url:
            out.append("base URL")
        if self.preset.needs_key and not self.api_key:
            out.append("API key")
        for role in ("chat_model", "extract_model", "embed_model"):
            if not getattr(self, role):
                out.append(role.replace("_", " "))
        if self.ca_bundle and not Path(self.ca_bundle).expanduser().is_file():
            out.append(f"CA certificate file ({self.ca_bundle})")
        return out


class SettingsUpdate(BaseModel):
    """What the UI sends. `api_key=None` keeps the stored key; the key is never sent back."""
    provider: str
    base_url: str = ""
    project_id: str = ""
    api_key: str | None = None
    chat_model: str = ""
    extract_model: str = ""
    embed_model: str = ""
    embed_dim: int = 0
    ca_bundle: str = ""


# --- .env file ------------------------------------------------------------------------------------------

LINE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$")


def env_path() -> Path:
    return Path(os.environ.get("MINI_CORTEX_ENV") or REPO_ROOT / ".env")


def _unquote(v: str) -> str:
    if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
        return v[1:-1]
    return re.sub(r"(?:^|\s+)#.*$", "", v)  # inline comment, also after an empty value


def read_env_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    out = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.lstrip().startswith("#"):
            continue
        m = LINE.match(line)
        if m:
            out[m[1]] = _unquote(m[2])
    return out


def write_env_file(path: Path, updates: Mapping[str, str | None]) -> None:
    """Set (or, with None, remove) keys in place. Other lines and comments are kept as they are."""
    lines = path.read_text(encoding="utf-8").splitlines() if path.is_file() else []
    todo = dict(updates)
    out = []
    for line in lines:
        m = None if line.lstrip().startswith("#") else LINE.match(line)
        if m and m[1] in todo:
            value = todo.pop(m[1])
            if value is not None:
                out.append(f"{m[1]}={_quote(value)}")
            continue
        out.append(line)
    new = [f"{k}={_quote(v)}" for k, v in todo.items() if v is not None]
    if new:
        if out and out[-1].strip():
            out.append("")
        out += ["# mini-cortex LLM settings (written by the UI; safe to edit by hand)", *new]
    path.write_text("\n".join(out) + "\n", encoding="utf-8")
    path.chmod(0o600)


def _quote(v: str) -> str:
    return f'"{v}"' if re.search(r"[\s#'\"]", v) else v


# --- resolution -----------------------------------------------------------------------------------------

ENV_KEYS = ("LLM_PROVIDER", "LLM_BASE_URL", "LLM_API_KEY", "SCW_SECRET_KEY", "SCW_PROJECT_ID",
            "CHAT_MODEL", "EXTRACT_MODEL", "EMBED_MODEL", "EMBED_DIM", "EMBED_BASE_URL", "LLM_CA_BUNDLE")


def raw_values(path: Path | None = None, updates: Mapping[str, str | None] | None = None,
               environ: Mapping[str, str] | None = None) -> tuple[dict[str, str], list[str]]:
    """The .env file (with pending `updates`) overlaid with real environment variables, plus the names the
    environment overrides."""
    environ = os.environ if environ is None else environ
    values = read_env_file(path or env_path())
    for k, val in (updates or {}).items():
        if val is None:
            values.pop(k, None)
        else:
            values[k] = val
    overridden = [k for k in ENV_KEYS if k in environ and environ[k] != values.get(k)]
    values.update({k: environ[k] for k in ENV_KEYS if k in environ})
    return values, overridden


def resolve(v: Mapping[str, str]) -> LLMSettings:
    provider = v.get("LLM_PROVIDER") or ("scaleway" if v.get("SCW_SECRET_KEY") or not v.get("LLM_BASE_URL") else "custom")
    preset = PRESETS.get(provider, PRESETS["custom"])
    if provider == "scaleway":
        project = v.get("SCW_PROJECT_ID", "")
        base_url = f"https://api.scaleway.ai/{project}/v1" if project else SCALEWAY_URL
        api_key = v.get("SCW_SECRET_KEY", "")
    else:
        project = ""
        base_url = v.get("LLM_BASE_URL") or preset.base_url
        api_key = v.get("LLM_API_KEY", "")
    try:
        dim = int(v.get("EMBED_DIM") or preset.embed_dim)
    except ValueError:
        dim = preset.embed_dim
    return LLMSettings(
        provider=provider, base_url=base_url.rstrip("/"), api_key=api_key, project_id=project,
        chat_model=v.get("CHAT_MODEL") or preset.chat_model,
        extract_model=v.get("EXTRACT_MODEL") or preset.extract_model,
        embed_model=v.get("EMBED_MODEL") or preset.embed_model,
        embed_dim=dim, embed_base_url=v.get("EMBED_BASE_URL", "").rstrip("/"), ca_bundle=v.get("LLM_CA_BUNDLE", ""))


def update_to_env(u: SettingsUpdate) -> dict[str, str | None]:
    """The .env changes for an update. Scaleway writes SCW_*, the others LLM_*; a missing key keeps the stored one."""
    out: dict[str, str | None] = {
        "LLM_PROVIDER": u.provider,
        "CHAT_MODEL": u.chat_model.strip(), "EXTRACT_MODEL": u.extract_model.strip(),
        # 0 is a choice too ("model default"): written out, so the preset's size doesn't quietly come back.
        "EMBED_MODEL": u.embed_model.strip(), "EMBED_DIM": str(u.embed_dim),
        "LLM_CA_BUNDLE": u.ca_bundle.strip() or None,
    }
    if u.provider == "scaleway":
        out["SCW_PROJECT_ID"] = u.project_id.strip() or None
        if u.api_key:
            out["SCW_SECRET_KEY"] = u.api_key.strip()
    else:
        out["LLM_BASE_URL"] = u.base_url.strip() or None
        if u.api_key:
            out["LLM_API_KEY"] = u.api_key.strip()
    return out


def load_settings(path: Path | None = None) -> LLMSettings:
    return resolve(raw_values(path)[0])


def draft_settings(u: SettingsUpdate, path: Path | None = None) -> LLMSettings:
    """Settings as they would be after saving `u`, without writing anything (for "Test" and the model list)."""
    return resolve(raw_values(path, update_to_env(u))[0])


def save_settings(u: SettingsUpdate, path: Path | None = None) -> None:
    write_env_file(path or env_path(), update_to_env(u))


def host(url: str) -> str:
    return urlsplit(url).netloc.lower()


def key_follows(u: SettingsUpdate, path: Path | None = None) -> bool:
    """May the stored key be used for `u` without re-entering it? Only for the same provider and host: otherwise a
    typo (or anyone who can edit the form) would send the saved key to a different server."""
    if u.api_key:
        return True
    saved = load_settings(path)
    return u.provider == saved.provider and host(draft_settings(u, path).base_url) == host(saved.base_url)


def key_hint(key: str) -> str:
    return f"••••{key[-4:]}" if len(key) >= 8 else ("••••" if key else "")
