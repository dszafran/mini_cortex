"""Start the enabled connectors as child processes and keep them running.

Each connector is `connectors/<name>_adapter.py serve`, run in the connectors' own uv project (its dependencies are not
the app's). The child dials back into our hub via HUB_URL/CONNECTOR_TOKEN; everything else (SLACK_*, REPLAY_*, ...)
is inherited from our environment.

Plain threads and `subprocess`, not asyncio: shutdown must work from a `finally` after the event loop is gone, on
Windows as well as macOS/Linux. Children run in their own process group so Ctrl+C reaches only us; we then stop them,
instead of them dying on the signal and the supervisor restarting them mid-shutdown.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path

from .llm_config import ENV_KEYS, REPO_ROOT, env_path, read_env_file

log = logging.getLogger("launcher")
CONNECTORS_DIR = REPO_ROOT / "connectors"
CONFIG_ERROR = 78            # connectors/source_adapter.py: a setting is missing or wrong, restarting won't help
MAX_RESTARTS = 3             # in a row; a run that lasted STABLE_S seconds resets the count
STABLE_S = 60.0
Command = Callable[[str], list[str]]


def adapter_path(name: str, root: Path = CONNECTORS_DIR) -> Path:
    return root / f"{name}_adapter.py"


def available(root: Path = CONNECTORS_DIR) -> list[str]:
    names = (p.stem.removesuffix("_adapter") for p in root.glob("*_adapter.py"))
    return sorted(n for n in names if n != "source")        # source_adapter.py is the base class, not a source


def default_connectors(root: Path = CONNECTORS_DIR) -> list[str]:
    """The offline replay story when it is there: the app shows something without any account or key."""
    return ["replay"] if adapter_path("replay", root).exists() else []


def from_settings() -> str | None:
    """MINI_CORTEX_CONNECTORS from the environment, else from the app's .env, so `replay,slack` can be set once."""
    return os.environ.get("MINI_CORTEX_CONNECTORS", read_env_file(env_path()).get("MINI_CORTEX_CONNECTORS"))


def why(names: list[str], cli: list[str] | None, env: str | None, root: Path = CONNECTORS_DIR) -> str:
    """One startup line: which connectors run, why, and how to change it (--connector replaces the default)."""
    if cli:
        source = "from --connector"
    elif env is not None:
        source = "from MINI_CORTEX_CONNECTORS"
    else:
        source = "the default; for more: --connector NAME or MINI_CORTEX_CONNECTORS=replay,slack in .env"
    left_out = [n for n in default_connectors(root) if n not in names]
    off = f" · {', '.join(left_out)} is off (list it too to keep it)" if left_out else ""
    return f"connectors: {', '.join(names) or 'none'} ({source}){off}"


def enabled(cli: list[str] | None, env: str | None, root: Path = CONNECTORS_DIR) -> list[str]:
    """--connector beats MINI_CORTEX_CONNECTORS (an empty value = none) beats the default. "none" disables all."""
    if cli:
        names = [n for c in cli for n in c.split(",")]
    elif env is not None:
        names = env.split(",")
    else:
        names = default_connectors(root)
    names = [n.strip() for n in names if n.strip() and n.strip() != "none"]
    return list(dict.fromkeys(names))


def backoff(failures: int) -> float:
    return 2.0 ** failures


def uv_command(name: str) -> list[str]:
    uv = os.environ.get("UV") or shutil.which("uv") or "uv"      # `uv run` sets UV to its own binary
    return [uv, "run", "--quiet", "--project", str(CONNECTORS_DIR), "python", str(adapter_path(name)), "serve"]


class Child:
    def __init__(self, name: str, argv: list[str], env: dict[str, str], out: Callable[[str], None]):
        self.name, self.argv, self.env, self.out = name, argv, env, out
        self.proc: subprocess.Popen | None = None
        self.starts = 0
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self._supervise, name=f"connector-{name}", daemon=True)

    def _spawn(self) -> subprocess.Popen:
        group = ({"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if sys.platform == "win32"
                 else {"start_new_session": True})
        self.starts += 1
        return subprocess.Popen(self.argv, env=self.env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace",
                                bufsize=1, **group)

    def _supervise(self) -> None:
        failures = 0
        while not self.stopping.is_set():
            started = time.monotonic()
            try:
                self.proc = self._spawn()
            except OSError as e:
                self.out(f"[{self.name}] could not start: {e}")
                return
            assert self.proc.stdout is not None
            for line in self.proc.stdout:
                self.out(f"[{self.name}] {line.rstrip()}")
            code = self.proc.wait()
            if self.stopping.is_set():
                return
            if code == CONFIG_ERROR:
                self.out(f"[{self.name}] stopped: fix the setting above (in .env or connectors/.env), "
                         "then restart mini-cortex")
                return
            failures = 0 if time.monotonic() - started > STABLE_S else failures + 1
            if failures > MAX_RESTARTS:
                self.out(f"[{self.name}] exited with code {code}; gave up after {MAX_RESTARTS} restarts")
                return
            delay = backoff(failures)
            self.out(f"[{self.name}] exited with code {code}; restarting in {delay} s")
            if self.stopping.wait(delay):
                return

    def stop(self, timeout: float = 5) -> None:
        self.stopping.set()
        p = self.proc
        if p and p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        self.thread.join(timeout)


class Launcher:
    def __init__(self, names: list[str], hub_url: str, token: str, command: Command = uv_command,
                 root: Path = CONNECTORS_DIR, out: Callable[[str], None] = lambda s: print(s, flush=True)):
        self.names, self.hub_url, self.token, self.command, self.root, self.out = names, hub_url, token, command, root, out
        self.children: list[Child] = []

    def start(self) -> None:
        # Connector settings (REPLAY_START, SLACK_TOKEN, ...) may sit in the app's .env, wherever the app was started
        # from; real environment variables still win. The model settings stay with the app: connectors need no key.
        from_file = {k: v for k, v in read_env_file(env_path()).items() if k not in ENV_KEYS}
        env = {**from_file, **os.environ, "HUB_URL": self.hub_url, "CONNECTOR_TOKEN": self.token,
               "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}
        env.pop("VIRTUAL_ENV", None)        # ours; uv would warn that it does not match the connectors' project
        for name in self.names:
            if not adapter_path(name, self.root).exists():
                log.warning("no connector %r (%s missing); available: %s", name, adapter_path(name, self.root).name,
                            ", ".join(available(self.root)) or "none")
                continue
            child = Child(name, self.command(name), env, self.out)
            child.thread.start()
            self.children.append(child)
            log.info("started connector %r", name)

    def stop(self) -> None:
        for c in self.children:
            c.stopping.set()
        for c in self.children:
            c.stop()
        self.children.clear()
