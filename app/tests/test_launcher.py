"""The connector launcher with a tiny fake adapter: env handed over, log lines prefixed, restarts capped, clean stop."""

import sys
import time

from mini_cortex import launcher

FAKE = '''
import os, sys, time
print("hub", os.environ["HUB_URL"], "token", os.environ["CONNECTOR_TOKEN"], flush=True)
if os.environ.get("FAKE_MODE") == "crash":
    sys.exit(3)
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
    assert launcher.enabled(None, None, tmp_path) == ["replay"]          # default when the replay story exists
    assert launcher.enabled(None, "", tmp_path) == []                    # env set but empty = none
    assert launcher.enabled(None, "slack, replay,slack", tmp_path) == ["slack", "replay"]
    assert launcher.enabled(["slack", "x,y"], "replay", tmp_path) == ["slack", "x", "y"]
    assert launcher.enabled(["none"], None, tmp_path) == []


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
