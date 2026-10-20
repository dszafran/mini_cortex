"""Snapshot save -> load round trip, the embedding-model check, and the refusal while the app runs."""

import socket

import pytest

from mini_cortex import snapshot
from mini_cortex.llm_config import LLMSettings
from mini_cortex.store import PacketStore, StateDB

SETTINGS = LLMSettings(provider="custom", base_url="http://x/v1", embed_model="emb-a", embed_dim=4)
ITEM = {"source_id": "C1:1", "timestamp": "2026-10-14T06:14:00Z", "content": "backhaul backup is on order",
        "thread_id": "incidents"}
PACKET = {"id": "pk_1", "type": "fact", "title": "Backhaul backup", "content": "The backhaul backup is on order.",
          "created_at": "2026-10-14T06:20:00Z", "updated_at": "2026-10-14T06:20:00Z"}


def make_memory(data):
    StateDB(data / "state.sqlite").upsert_raw("slack", "channels", [ITEM])
    PacketStore(data / "lance").upsert(PACKET, [0.1, 0.2, 0.3, 0.4])
    (data / "items.jsonl").write_text("debug log, not packed\n")


def test_round_trip(tmp_path):
    data, root = tmp_path / "data", tmp_path / "snapshots"
    make_memory(data)
    path, manifest = snapshot.save("day1", data, SETTINGS, root)
    assert path == root / "day1.tar.gz" and snapshot.listing(root) == ["day1"]
    assert (manifest["items"], manifest["packets"], manifest["vector_dim"], manifest["embed_model"]) == \
        (1, 1, 4, "emb-a")

    other = tmp_path / "other"
    StateDB(other / "state.sqlite")                          # some unrelated memory, kept aside on load
    m, warning = snapshot.load("day1", other, SETTINGS, root)
    assert m["name"] == "day1" and warning is None
    assert StateDB(other / "state.sqlite").raw_item("slack", "C1:1")["item"]["content"] == ITEM["content"]
    assert PacketStore(other / "lance", 4).get("pk_1")["title"] == "Backhaul backup"
    assert not (other / "items.jsonl").exists() and (tmp_path / "other.prev" / "state.sqlite").exists()


def test_warns_on_another_embedding_model(tmp_path):
    data, root = tmp_path / "data", tmp_path / "snapshots"
    make_memory(data)
    snapshot.save("day1", data, SETTINGS, root)
    _, warning = snapshot.load("day1", data, SETTINGS.model_copy(update={"embed_model": "emb-b"}), root)
    assert "emb-a" in warning and "emb-b" in warning
    _, warning = snapshot.load("day1", data, SETTINGS.model_copy(update={"embed_dim": 8}), root)
    assert "EMBED_DIM=4" in warning


def test_refuses_while_the_app_runs(tmp_path):
    data, root = tmp_path / "data", tmp_path / "snapshots"
    make_memory(data)
    snapshot.save("day1", data, SETTINGS, root)
    with socket.socket() as srv:                             # stands in for the running server
        srv.bind(("127.0.0.1", 0))
        srv.listen()
        snapshot.write_lock(data, "127.0.0.1", srv.getsockname()[1])
        with pytest.raises(snapshot.SnapshotError, match="running"):
            snapshot.load("day1", data, SETTINGS, root)
        with pytest.raises(snapshot.SnapshotError, match="running"):
            snapshot.save("day2", data, SETTINGS, root)
    snapshot.load("day1", data, SETTINGS, root)              # port closed: the lock is stale, load goes ahead


def test_bad_names(tmp_path):
    for name in ("../x", "a/b", ".hidden", ""):
        with pytest.raises(snapshot.SnapshotError):
            snapshot.save(name, tmp_path, SETTINGS, tmp_path / "snapshots")


def test_a_gateway_prefix_is_the_same_model(tmp_path):
    """Built behind a gateway that names the model `generative-apis/emb-a`, loaded by someone who calls it `emb-a`."""
    data, root = tmp_path / "data", tmp_path / "snapshots"
    make_memory(data)
    snapshot.save("day1", data, SETTINGS.model_copy(update={"embed_model": "generative-apis/emb-a"}), root)
    _, warning = snapshot.load("day1", data, SETTINGS, root)
    assert warning is None
    _, warning = snapshot.load("day1", data, SETTINGS.model_copy(update={"embed_model": "emb-b"}), root)
    assert "generative-apis/emb-a" in warning
    _, warning = snapshot.load("day1", data, SETTINGS.model_copy(update={"embed_model": "other/emb-a"}), root)
    assert "other/emb-a" in warning                          # another gateway's emb-a may be another model
