import os

from pathlib import Path

import httpx
import openai
from fastapi import FastAPI

from conftest import GOOD_KEY
from mini_cortex.doctor import Probe, explain, run_checks
import pytest

from mini_cortex.llm import LLM, EmbeddingSize, OutOfCredits, Stalled, cut, reset_seconds
from mini_cortex.llm_config import (OPENROUTER_URL, LLMSettings, SettingsUpdate, load_settings, raw_values,
                                    read_env_file, resolve, save_settings, write_env_file)
from mini_cortex.pipeline import is_fatal
from mini_cortex.llm_routes import make_router


REPO = Path(__file__).resolve().parents[2]


def settings(url: str, key: str = GOOD_KEY, embed_model_: str = "qwen3-embedding-8b", **kw) -> LLMSettings:
    return LLMSettings(provider="custom", base_url=url, api_key=key, chat_model="glm-5.3",
                       extract_model="deepseek-v4.1-flash", embed_model=embed_model_, embed_dim=1024, **kw)


# --- config ---------------------------------------------------------------------------------------------

def test_scaleway_url_from_project_and_preset_models():
    s = resolve({"SCW_SECRET_KEY": "k" * 36, "SCW_PROJECT_ID": "p-123"})
    assert s.provider == "scaleway"
    assert s.base_url == "https://api.scaleway.ai/p-123/v1"
    assert (s.chat_model, s.embed_model, s.embed_dim) == ("qwen3-235b-a22b-instruct-2507", "qwen3-embedding-8b", 1024)
    assert resolve({}).base_url == "https://api.scaleway.ai/v1"           # default project
    assert resolve({"LLM_BASE_URL": "http://x/v1"}).provider == "custom"


def test_env_file_roundtrip_keeps_other_lines(tmp_path):
    p = tmp_path / ".env"
    p.write_text("# slack\nA_BOT_TOKEN=xoxb-1\nCHAT_MODEL=old\n")
    write_env_file(p, {"CHAT_MODEL": "glm-5.3", "SCW_SECRET_KEY": "secret value", "EMBED_DIM": None})
    text = p.read_text()
    assert text.startswith("# slack\nA_BOT_TOKEN=xoxb-1\nCHAT_MODEL=glm-5.3\n")
    assert read_env_file(p) == {"A_BOT_TOKEN": "xoxb-1", "CHAT_MODEL": "glm-5.3", "SCW_SECRET_KEY": "secret value"}
    assert oct(p.stat().st_mode & 0o777) == "0o600"


def test_env_example_parses(tmp_path):
    values = read_env_file(REPO / ".env.example")
    assert values["SCW_PROJECT_ID"] == "" and values["CHAT_MODEL"] == values["EXTRACT_MODEL"] == "qwen3-235b-a22b-instruct-2507" and values["EMBED_DIM"] == "1024"
    assert resolve(values).base_url == "https://api.scaleway.ai/v1"


def test_embedding_size_zero_means_model_default(env_file):
    """Tune → Models sends 0 for "model default": it must not turn back into the preset's 1024."""
    scaleway = {"provider": "scaleway", "api_key": "k" * 36, "embed_model": "qwen3-embedding-8b"}
    save_settings(SettingsUpdate(**scaleway, embed_dim=0))
    assert load_settings().embed_dim == 0
    save_settings(SettingsUpdate(**scaleway, embed_dim=1024))
    assert load_settings().embed_dim == 1024
    assert resolve({"SCW_SECRET_KEY": "k" * 36}).embed_dim == 1024      # nothing chosen yet: the preset's size


def test_extra_ca_bundle(tmp_path):
    import certifi
    from mini_cortex.llm import ssl_context
    assert ssl_context(certifi.where()).cert_store_stats()["x509_ca"] > ssl_context().cert_store_stats()["x509_ca"] / 2
    s = resolve({"LLM_PROVIDER": "custom", "LLM_BASE_URL": "http://x/v1", "LLM_API_KEY": "k",
                 "CHAT_MODEL": "a", "EXTRACT_MODEL": "b", "EMBED_MODEL": "c", "LLM_CA_BUNDLE": str(tmp_path / "nope.pem")})
    assert s.missing() == [f"CA certificate file ({tmp_path / 'nope.pem'})"]


def test_save_without_key_keeps_stored_key(env_file):
    save_settings(SettingsUpdate(provider="scaleway", api_key="first-key-abcd", project_id="p1", chat_model="a",
                                 extract_model="b", embed_model="c", embed_dim=1024))
    save_settings(SettingsUpdate(provider="scaleway", project_id="p2", chat_model="a2", extract_model="b",
                                 embed_model="c", embed_dim=1024))
    s = load_settings()
    assert (s.api_key, s.project_id, s.chat_model) == ("first-key-abcd", "p2", "a2")


def test_environment_wins_and_is_reported(env_file, monkeypatch):
    env_file.write_text("CHAT_MODEL=from-file\n")
    monkeypatch.setenv("CHAT_MODEL", "from-env")
    assert load_settings().chat_model == "from-env"
    assert raw_values()[1] == ["CHAT_MODEL"]


# --- client against the stub ----------------------------------------------------------------------------

async def test_extract_native_and_fallback(upstream):
    up, url = upstream
    llm = LLM(settings(url))
    q = [{"role": "user", "content": "Warsaw"}]
    assert (await llm.extract(q, Probe)).city == "Warsaw"
    assert llm.native_json is True

    up.no_json_schema = True
    llm = LLM(settings(url))
    assert (await llm.extract(q, Probe)).landmarks[0] == "Palace of Culture"   # fenced JSON parsed
    assert llm.native_json is False


async def test_deadline_cuts_a_stalled_call_and_retries(upstream):
    up, url = upstream
    up.stall = 1                                       # first chat request hangs for 5 s
    llm = LLM(settings(url), timeout=30, deadline=0.5, stall_retries=1)
    assert (await llm.chat([{"role": "user", "content": "hi"}])).content == "pong"

    up.stall = 5
    with pytest.raises(Stalled):
        await LLM(settings(url), timeout=30, deadline=0.3, stall_retries=1).chat([{"role": "user", "content": "x"}])


def test_openrouter_preset_keeps_its_own_key(env_file):
    """The backup provider: a fixed URL, its own key variable, so switching back to Scaleway needs no re-entry."""
    s = resolve({"LLM_PROVIDER": "openrouter", "OPENROUTER_API_KEY": "sk-or-1", "LLM_BASE_URL": "http://old/v1"})
    assert (s.base_url, s.api_key, s.embed_dim) == (OPENROUTER_URL, "sk-or-1", 1024)
    assert (s.chat_model, s.embed_model) == ("qwen/qwen3-235b-a22b-2507", "qwen/qwen3-embedding-8b")
    save_settings(SettingsUpdate(provider="scaleway", api_key="k" * 36, embed_model="qwen3-embedding-8b", embed_dim=1024))
    save_settings(SettingsUpdate(provider="openrouter", api_key="sk-or-2", embed_model="qwen/qwen3-embedding-8b",
                                 embed_dim=1024, base_url=OPENROUTER_URL))
    stored = read_env_file(env_file)
    assert stored["OPENROUTER_API_KEY"] == "sk-or-2" and stored["SCW_SECRET_KEY"] == "k" * 36
    assert "LLM_BASE_URL" not in stored and load_settings().api_key == "sk-or-2"
    save_settings(SettingsUpdate(provider="scaleway", embed_model="qwen3-embedding-8b", embed_dim=1024))
    assert load_settings().api_key == "k" * 36                          # back without re-entering the key


async def test_openrouter_routing_and_model_list(upstream):
    up, url = upstream
    llm = LLM(settings(url).model_copy(update={"provider": "openrouter"}))
    await llm.chat([{"role": "user", "content": "hi"}])
    assert up.requests[-1]["provider"] == {"require_parameters": True}
    await llm.extract([{"role": "user", "content": "Warsaw?"}], Probe)
    assert up.requests[-1]["provider"] == {"require_parameters": True}
    await llm.embed(["hello"])
    assert up.requests[-1]["provider"]["order"][0] == "nebius"
    assert "qwen/qwen3-embedding-8b" in await llm.list_models()
    await LLM(settings(url)).chat([{"role": "user", "content": "hi"}])
    assert "provider" not in up.requests[-1]                              # other providers get plain requests


async def test_no_credit_stops_the_run(upstream):
    up, url = upstream
    up.no_credit = True
    with pytest.raises(OutOfCredits) as e:
        await LLM(settings(url)).chat([{"role": "user", "content": "hi"}])
    assert is_fatal(e.value)                                              # Process stops instead of failing each window
    checks = {c.name: c for c in await run_checks(settings(url))}
    assert checks["chat"].status == "fail" and "credit" in checks["chat"].fix


async def test_rate_limit_is_waited_out(upstream, monkeypatch):
    up, url = upstream
    monkeypatch.setattr("mini_cortex.llm.random.uniform", lambda a, b: 0.0)
    up.rate_limited = 4                                # more than the SDK's own 2 retries
    llm = LLM(settings(url), max_retries=2, rate_limit_wait=30)
    assert (await llm.chat([{"role": "user", "content": "hi"}])).content == "pong"
    assert llm.rate_limited == 1 and up.rate_limited == 0
    up.rate_limited = 5
    with pytest.raises(openai.RateLimitError):         # the doctor: no waiting
        await LLM(settings(url), max_retries=0).chat([{"role": "user", "content": "hi"}])


def test_reset_seconds_reads_scaleway_headers():
    def err(**headers):
        return openai.RateLimitError("x", response=httpx.Response(429, headers=headers,
                                     request=httpx.Request("POST", "http://x")), body=None)
    assert reset_seconds(err(**{"x-ratelimit-reset-requests": "59s985ms"})) == pytest.approx(59.985)
    assert reset_seconds(err(**{"x-ratelimit-reset-requests": "1m2s", "x-ratelimit-reset-tokens": "0ms"})) == 62
    assert reset_seconds(err(**{"retry-after": "7"})) == 7
    assert reset_seconds(err()) is None


async def test_vectors_are_cut_when_the_server_ignores_dimensions(upstream):
    up, url = upstream
    up.ignore_dims = True
    llm = LLM(settings(url))
    [v] = await llm.embed(["hello"])
    assert len(v) == 1024 and llm.cut_from == 4096
    assert cut([3.0, 4.0, 12.0], 2) == [0.6, 0.8]                        # first n values, renormalized
    gateway = LLM(settings(url, embed_model_="generative-apis/qwen3-embedding-8b"))   # same model behind a gateway
    assert len((await gateway.embed(["hello"]))[0]) == 1024


async def test_a_fixed_size_model_is_never_cut(upstream):
    up, url = upstream
    up.ignore_dims = True
    with pytest.raises(EmbeddingSize, match="Set the embedding size to 4096"):
        await LLM(settings(url, embed_model_="bge-multilingual-gemma2")).embed(["hello"])
    checks = {c.name: c for c in await run_checks(settings(url, embed_model_="bge-multilingual-gemma2"))}
    assert checks["embeddings"].status == "fail" and "4096" in checks["embeddings"].fix


async def test_embed_prefix_and_dimensions(upstream):
    up, url = upstream
    [v] = await LLM(settings(url)).embed(["hello"], kind="query")
    assert len(v) == 1024
    assert up.requests[-1]["input"] == ["search_query: hello"]
    assert up.requests[-1]["dimensions"] == 1024


# --- doctor ---------------------------------------------------------------------------------------------

async def test_doctor_all_green(upstream):
    _, url = upstream
    checks = {c.name: c for c in await run_checks(settings(url))}
    assert [c.status for c in checks.values()] == ["ok"] * 6, checks
    assert "get_current_time" in checks["tool calling"].detail


async def test_doctor_warns_and_fails_with_fixes(upstream):
    up, url = upstream
    up.no_json_schema = up.ignore_dims = up.no_tools = True
    checks = {c.name: c for c in await run_checks(settings(url))}
    assert checks["structured output"].status == "warn"
    assert checks["embeddings"].status == "ok" and "cut here from 4096" in checks["embeddings"].detail
    assert checks["tool calling"].status == "fail" and "tool" in checks["tool calling"].fix


async def test_doctor_retries_a_stalled_call(upstream):
    up, url = upstream
    up.stall = 1
    checks = {c.name: c for c in await run_checks(settings(url), timeout=0.5)}
    stalled = [c for c in checks.values() if "timed-out" in c.detail]
    assert len(stalled) == 1 and stalled[0].status == "warn" and "retries" in stalled[0].fix
    assert all(c.status in ("ok", "warn") for c in checks.values())


async def test_doctor_bad_key_stops_early(upstream):
    _, url = upstream
    checks = await run_checks(settings(url, key="sk-wrong"))
    assert [c.name for c in checks] == ["settings", "models"]
    assert checks[-1].status == "fail" and "rejected" in checks[-1].fix


async def test_doctor_missing_settings():
    [c] = await run_checks(LLMSettings(provider="scaleway", chat_model="x", extract_model="y", embed_model="z"))
    assert c.status == "fail" and "API key" in c.detail


async def test_doctor_unreachable():
    checks = await run_checks(settings("http://127.0.0.1:9/v1"))
    assert checks[-1].status == "fail" and "Cannot reach" in checks[-1].fix


def status_error(cls, status: int, message: str):
    return cls(message, response=httpx.Response(status, request=httpx.Request("POST", "http://x/v1")), body=None)


def test_explain_names_expiry_for_a_refused_key_and_the_model_for_a_401():
    scw = LLMSettings(provider="scaleway", api_key="k", project_id="p-1")
    refused = status_error(openai.PermissionDeniedError, 403, "insufficient permissions to access the resource")
    assert "expired or been revoked" in explain(refused, scw) and "IAM" in explain(refused, scw)
    assert "Project ID" in explain(refused, LLMSettings(provider="scaleway", api_key="k"))   # no project: say that first
    unknown = status_error(openai.AuthenticationError, 401,
                           "The model `glm-5.3` does not exist or is not authorized for this API key.")
    assert "not available for this key" in explain(unknown, settings("http://x/v1"), "glm-5.3")
    assert "rejected" in explain(status_error(openai.AuthenticationError, 401, "bad key"), settings("http://x/v1"))


# --- routes ---------------------------------------------------------------------------------------------

async def test_routes_never_return_the_key(upstream, env_file):
    _, url = upstream
    app = FastAPI()
    app.include_router(make_router())
    api = httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://ui")

    first = (await api.get("/api/llm/settings")).json()
    assert first["provider"] == "scaleway" and "API key" in first["missing"]
    assert [p["id"] for p in first["presets"]] == ["scaleway", "openrouter"]     # no local or internal options

    draft = {"provider": "custom", "base_url": url, "api_key": GOOD_KEY, "chat_model": "glm-5.3",
             "extract_model": "deepseek-v4.1-flash", "embed_model": "qwen3-embedding-8b", "embed_dim": 1024}
    models = (await api.post("/api/llm/models", json=draft)).json()["models"]
    assert {"id": "qwen3-embedding-8b", "kind": "embed"} in models
    assert not env_file.exists()                                   # listing/testing saves nothing

    test = (await api.post("/api/llm/test", json=draft)).json()
    assert test["ok"], test

    saved = (await api.put("/api/llm/settings", json=draft)).json()
    assert saved["key_set"] and saved["key_hint"] == "••••1234" and saved["missing"] == []
    assert GOOD_KEY not in str(saved)
    assert read_env_file(env_file)["LLM_API_KEY"] == GOOD_KEY

    # Test again without resending the key: the stored one is used.
    test = (await api.post("/api/llm/test", json={**draft, "api_key": None})).json()
    assert test["ok"], test

    bad = await api.post("/api/llm/models", json={**draft, "api_key": "sk-wrong"})
    assert bad.status_code == 502 and "rejected" in bad.json()["detail"]["fix"]
    assert os.environ.get("LLM_API_KEY") is None

    # The saved key never follows a new host: a typo'd or hostile base URL must not receive it.
    elsewhere = {**draft, "api_key": None, "base_url": "http://127.0.0.1:9/v1"}
    assert (await api.post("/api/llm/models", json=elsewhere)).status_code == 400
    assert (await api.put("/api/llm/settings", json=elsewhere)).status_code == 422
    assert (await api.post("/api/llm/test", json=elsewhere)).json()["checks"][0]["status"] == "fail"

    url_as_project = await api.put("/api/llm/settings", json={**draft, "provider": "scaleway", "project_id": url})
    assert url_as_project.status_code == 422
