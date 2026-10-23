"""Shared test setup for the connectors."""

import re
from pathlib import Path

import pytest

LATER_STEPS = {           # test -> the workshop step whose exercise it needs (replay_adapter.fetch_window)
    name: 1 for name in ("test_against_dev_hub", "test_all_story_items_are_served_and_ids_are_stable",
                         "test_channel_messages_and_threads", "test_cues_push_on_schedule_and_on_demand",
                         "test_dms_mail_notes", "test_future_items_stay_hidden", "test_slice_env",
                         "test_window_is_half_open_and_pages")
}
# --- workshop step branches -----------------------------------------------------------------------------------------
# A step branch also carries the later steps' exercises as TODOs. Their tests would fail there for a reason that isn't
# this step's, so they're skipped with the step they belong to; this step's own tests still fail until it's solved.
# The current step comes from STEP.md (none on main: nothing is skipped).

def current_step() -> int | None:
    step_md = Path(__file__).resolve().parents[2] / "STEP.md"
    m = re.match(r"# Step (\d+)", step_md.read_text(encoding="utf-8")) if step_md.is_file() else None
    return int(m[1]) if m else None


def pytest_collection_modifyitems(config, items):
    now = current_step()
    if now is None:
        return
    for item in items:
        step = LATER_STEPS.get(item.name.split("[")[0])
        if step is not None and step > now:
            item.add_marker(pytest.mark.skip(reason=f"comes in workshop step {step}"))
