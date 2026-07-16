"""Node-level trace events emitted by the pipeline runner (Track 4 / ops step 6).

_execute_node emits running/succeeded/failed to an optional node_event_callback
the launcher wires under a ledger job, so LocalJobRunner writes job_node_runs
rows. No callback (plain connection runs) => no-op.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from subjective_abstract_data_source_package.SubjectivePipelineDataSource import (  # noqa: E402
    _SubjectiveDataSourcePipelineRunner,
    _V2PipelineNode,
)


def _runner_with_node(instance: Any, *, node_id: str = "n1", class_name: str = "DemoDataSource"):
    runner = object.__new__(_SubjectiveDataSourcePipelineRunner)
    node = _V2PipelineNode(node_id=node_id, class_name=class_name, connection_name=node_id, inputs={})
    node.instance = instance
    runner._emitted: list[dict] = []  # capture for assertions
    runner.node_event_callback = lambda event: runner._emitted.append(event)
    return runner, node


class _OkInstance:
    _config: dict = {}

    def subscribe(self, collector):  # noqa: D401
        pass

    def fetch(self):
        return None


class _BoomInstance(_OkInstance):
    def fetch(self):
        raise RuntimeError("node exploded")


def test_no_callback_is_a_noop() -> None:
    runner = object.__new__(_SubjectiveDataSourcePipelineRunner)
    node = _V2PipelineNode(node_id="n1", class_name="X", connection_name="n1", inputs={})
    # No node_event_callback attribute set -> must not raise.
    runner._emit_node_event(node, "running")


def test_running_and_status_events_captured() -> None:
    runner, node = _runner_with_node(_OkInstance())
    runner._emit_node_event(node, "running")
    runner._emit_node_event(node, "succeeded")
    assert [e["node_status"] for e in runner._emitted] == ["running", "succeeded"]
    assert runner._emitted[0]["node_id"] == "n1"
    assert runner._emitted[0]["node_class"] == "DemoDataSource"


def test_failed_event_carries_error() -> None:
    runner, node = _runner_with_node(_OkInstance())
    runner._emit_node_event(node, "failed", error="node exploded")
    assert runner._emitted[0]["node_status"] == "failed"
    assert runner._emitted[0]["error"] == "node exploded"


def test_callback_exception_never_propagates() -> None:
    runner = object.__new__(_SubjectiveDataSourcePipelineRunner)
    node = _V2PipelineNode(node_id="n1", class_name="X", connection_name="n1", inputs={})

    def boom(_event):
        raise ValueError("trace sink down")

    runner.node_event_callback = boom
    runner._emit_node_event(node, "running")  # swallowed; no raise
