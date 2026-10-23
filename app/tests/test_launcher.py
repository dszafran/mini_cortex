"""The connector launcher with a tiny fake adapter: env handed over, log lines prefixed, restarts capped, clean stop."""

import signal
import sys
import time

import pytest

from mini_cortex import launcher, server

FAKE = '''
import os, sys, time
print("hub", os.environ["HUB_URL"], "token", os.environ["CONNECTOR_TOKEN"], flush=True)
if os.environ.get("FAKE_MODE") == "crash":
    sys.exit(3)
if os.environ.get("FAKE_MODE") == "config":
    print("FAKE_TOKEN is not set", flush=True)
    sys.exit(78)
time.sleep(60)
'''


def wait_for(cond, timeout=10.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        time.sleep(0.05)
    return False


def fake_launcher(tmp_path, names, lines):
    (tmp_path / "fake_adapter.py").write_text(FAKE)
    cmd = lambda name: [sys.executable, str(launcher.adapter_path(name, tmp_path))]  # noqa: E731
    return launcher.Launcher(names, "ws://127.0.0.1:1/connectors", "tok", cmd, tmp_path, lines.append)


def test_enabled_precedence(tmp_path):
    assert launcher.enabled(None, None, tmp_path) == []
    (tmp_path / "replay_adapter.py").write_text("")
    assert launcher.enabled(None, None, tmp_path) == []                  # nothing by default: you wire a source
    assert launcher.enabled(None, "", tmp_path) == []                    # env set but empty = none
    assert launcher.enabled(None, "slack, replay,slack", tmp_path) == ["slack", "replay"]
    assert launcher.enabled(["slack", "x,y"], "replay", tmp_path) == ["slack", "x", "y"]
    assert launcher.enabled(["none"], None, tmp_path) == []


def test_startup_line_says_why_and_what_is_off(tmp_path, monkeypatch):
    (tmp_path / "replay_adapter.py").write_text("")
    none = launcher.why([], None, None, tmp_path)
    assert "no data comes in" in none and "MINI_CORTEX_CONNECTORS=slack" in none and "=replay" in none
    assert launcher.why(["slack"], ["slack"], None, tmp_path) == "connectors: slack (from --connector)"
    assert launcher.why(["replay", "slack"], None, "replay,slack", tmp_path).endswith("(from MINI_CORTEX_CONNECTORS)")
    env = tmp_path / ".env"
    env.write_text("MINI_CORTEX_CONNECTORS=replay,slack\n")
    monkeypatch.setenv("MINI_CORTEX_ENV", str(env))
    monkeypatch.delenv("MINI_CORTEX_CONNECTORS", raising=False)
    assert launcher.from_settings() == "replay,slack"                    # set once in .env
    monkeypatch.setenv("MINI_CORTEX_CONNECTORS", "slack")
    assert launcher.from_settings() == "slack"                           # the real environment wins


def test_starts_prefixes_and_stops(tmp_path):
    lines: list[str] = []
    run = fake_launcher(tmp_path, ["fake", "missing"], lines)
    run.start()
    assert [c.name for c in run.children] == ["fake"]                    # unknown names are skipped with a warning
    assert wait_for(lambda: lines)
    assert lines[0] == "[fake] hub ws://127.0.0.1:1/connectors token tok"
    proc = run.children[0].proc
    run.stop()
    assert proc.poll() is not None and run.children == []


def test_restarts_a_crashing_connector_then_gives_up(tmp_path, monkeypatch):
    monkeypatch.setenv("FAKE_MODE", "crash")
    monkeypatch.setattr(launcher, "MAX_RESTARTS", 2)
    monkeypatch.setattr(launcher, "backoff", lambda failures: 0.01)
    lines: list[str] = []
    run = fake_launcher(tmp_path, ["fake"], lines)
    run.start()
    child = run.children[0]
    assert wait_for(lambda: not child.thread.is_alive())
    assert child.starts == 3                                             # first run + 2 restarts
    assert "restarting in" in lines[1] and "gave up after 2 restarts" in lines[-1]
    run.stop()


def test_a_config_error_is_not_restarted(tmp_path, monkeypatch):
    monkeypatch.setenv("FAKE_MODE", "config")
    monkeypatch.setattr(launcher, "backoff", lambda failures: 0.01)
    lines: list[str] = []
    run = fake_launcher(tmp_path, ["fake"], lines)
    run.start()
    child = run.children[0]
    assert wait_for(lambda: not child.thread.is_alive())
    assert child.starts == 1 and "FAKE_TOKEN is not set" in lines[-2] and "fix the setting" in lines[-1]
    run.stop()


ENV_FAKE = '''
import os, time
print("start", os.environ.get("REPLAY_START"), "slack", os.environ.get("SLACK_TOKEN"),
      "key", os.environ.get("LLM_API_KEY", "-"), flush=True)
time.sleep(60)
'''


def test_connectors_get_the_app_env_but_not_the_model_key(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("REPLAY_START=09:30\nSLACK_TOKEN=xoxb-file\nLLM_API_KEY=sk-secret\n")
    monkeypatch.setenv("MINI_CORTEX_ENV", str(env))
    monkeypatch.setenv("SLACK_TOKEN", "xoxb-real-env")                   # a real variable beats the file
    monkeypatch.delenv("REPLAY_START", raising=False)
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    (tmp_path / "fake_adapter.py").write_text(ENV_FAKE)
    lines: list[str] = []
    cmd = lambda name: [sys.executable, str(launcher.adapter_path(name, tmp_path))]  # noqa: E731
    run = launcher.Launcher(["fake"], "ws://127.0.0.1:1/connectors", "tok", cmd, tmp_path, lines.append)
    run.start()
    assert wait_for(lambda: lines)
    run.stop()
    assert lines[0] == "[fake] start 09:30 slack xoxb-real-env key -"


@pytest.mark.skipif(not hasattr(signal, "SIGHUP"), reason="no SIGHUP on this platform")
def test_terminal_close_and_kill_stop_like_ctrl_c():
    """SIGHUP and SIGTERM become KeyboardInterrupt, so serve() reaches its `finally` and stops the connectors."""
    old = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGHUP)}
    try:
        server.stop_on_signals()
        for s in old:
            with pytest.raises(KeyboardInterrupt):
                signal.raise_signal(s)
    finally:
        for s, handler in old.items():
            signal.signal(s, handler)
