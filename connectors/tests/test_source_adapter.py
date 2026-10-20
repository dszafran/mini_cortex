"""The shared adapter command line: settings come from the .env next to the adapters, not the working directory."""

import os
from datetime import timedelta

import source_adapter
from connector_sdk import iso
from source_adapter import SourceAdapter, StreamSpec


class Notes(SourceAdapter):
    name = "notes"
    streams = [StreamSpec("notes", kind="note")]

    async def to_item(self, stream, native):
        return native

    async def fetch_window(self, stream, since, until):
        return [{"source_id": "notes:1", "timestamp": iso(until - timedelta(minutes=1)),
                 "content": os.environ["NOTE_TEXT"]}]


def test_main_reads_the_env_file_next_to_the_adapters(tmp_path, monkeypatch, capsys):
    adapters, elsewhere = tmp_path / "connectors", tmp_path / "repo-root"
    adapters.mkdir()
    elsewhere.mkdir()
    (adapters / ".env").write_text("NOTE_TEXT=from connectors/.env\n")
    (elsewhere / ".env").write_text("NOTE_TEXT=from the working directory\n")
    monkeypatch.setattr(source_adapter, "ENV_FILE", adapters / ".env")
    monkeypatch.chdir(elsewhere)                          # where `uv run mini-cortex` was started
    monkeypatch.setenv("NOTE_TEXT", "")
    monkeypatch.delenv("NOTE_TEXT")                       # unset now, and unset again after the test
    source_adapter.main(Notes, ["fetch", "--days", "1"])
    assert "from connectors/.env" in capsys.readouterr().out
