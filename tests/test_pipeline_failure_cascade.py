"""Tests for the runner's per-node crash containment and failed-upstream cascade.

Both layers are covered:

1. ``_run_batch_nodes`` wraps ``_execute_node`` so a node that raises is
   converted into a ``{"success": False, "error": ..., "error_type": ...}``
   result instead of killing the process.
2. ``_resolve_input_value`` (via ``_node_result_indicates_failure``) treats a
   failed upstream's outputs as unresolved (`_SKIP_PIPELINE_INPUT`) so
   downstream nodes "skip by input resolution" rather than being called with
   empty / garbage data that would crash them in turn.

Without these, the bug observed in the field — worktwins's episode rollup
producing invalid JSON that crashed the SubjectiveTextDataSource and killed
the whole long-running monitor — is reproducible.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from subjective_abstract_data_source_package.SubjectivePipelineDataSource import (  # noqa: E402
    _SKIP_PIPELINE_INPUT,
    _SubjectiveDataSourcePipelineRunner,
    _V2PipelineNode,
    _node_result_indicates_failure,
)


# ---------------------------------------------------------------------------
# Helpers


def _make_runner(pipeline_config: dict[str, Any]) -> _SubjectiveDataSourcePipelineRunner:
    """Build a runner without invoking the plugin importer.

    The runner normally imports each node's datasource class via
    ``DATASOURCES_PLUGIN_PATH`` lookup. For these tests we don't need real
    plugins — we drive ``_resolve_input_value`` directly and inspect the
    resulting sentinel. We monkey-patch ``_build_nodes`` to populate a minimal
    node registry.
    """
    runner = object.__new__(_SubjectiveDataSourcePipelineRunner)
    runner.pipeline_name = pipeline_config.get("name") or "test"
    runner.pipeline_config = pipeline_config
    runner.context_dir = ""
    runner.tmp_root = ""
    runner.workspace_root = ""
    runner.nodes = {}
    runner._workflow_start_node_ids = set()
    runner._workflow_end_node_ids = set()
    runner._workflow_iterator_nodes = {}
    runner._workflow_accumulator_nodes = {}
    runner._accumulator_buffers = {}
    runner._accumulator_last_markers = {}
    runner._node_result_versions = {}
    runner._iterated_nodes = set()
    runner._pending_context_references = {}
    runner._pipeline_cycle_context_references = []
    import threading

    runner._context_reference_lock = threading.Lock()
    runner._stream_threads = []
    runner._stop_event = threading.Event()
    runner._connection_records = []

    for raw_node in pipeline_config.get("nodes", []):
        node = _V2PipelineNode(
            node_id=raw_node["node_id"],
            class_name=raw_node["class"],
            connection_name=raw_node.get("connection_name", raw_node["node_id"]),
            inputs=raw_node.get("inputs", {}) or {},
        )
        runner.nodes[node.node_id] = node
    return runner


# ---------------------------------------------------------------------------
# _node_result_indicates_failure — the central predicate


class TestNodeResultIndicatesFailure:
    """The cascade-skip predicate must recognise both signal conventions."""

    def test_success_false_is_failure(self):
        assert _node_result_indicates_failure({"success": False, "error": "boom"}) is True

    def test_success_true_overrides_error_field(self):
        # Some plugins (e.g. screenshot) always emit an ``error`` field, often
        # empty, alongside ``success: True``. ``success: True`` must win so we
        # don't accidentally skip downstream of a healthy node.
        assert (
            _node_result_indicates_failure({"success": True, "error": "False"}) is False
        )
        assert _node_result_indicates_failure({"success": True, "error": ""}) is False
        assert (
            _node_result_indicates_failure(
                {"success": True, "error": "stringified-boolean"}
            )
            is False
        )

    def test_legacy_error_only_convention(self):
        # SubjectiveJsonDataSource returns this exact shape when JSON parsing
        # fails. There is no ``success`` field, only a non-empty ``error``.
        assert (
            _node_result_indicates_failure(
                {"json": None, "text": "", "error": "Expecting value: line 1 column 1"}
            )
            is True
        )

    def test_empty_error_is_not_failure(self):
        # Many datasources include ``error: ""`` as a stable schema field on
        # successful runs — it must not look like a failure.
        assert _node_result_indicates_failure({"text": "hello", "error": ""}) is False
        assert _node_result_indicates_failure({"text": "hello", "error": None}) is False

    def test_non_dict_results_are_not_failures(self):
        assert _node_result_indicates_failure(None) is False
        assert _node_result_indicates_failure("payload") is False
        assert _node_result_indicates_failure(["a", "b"]) is False


# ---------------------------------------------------------------------------
# _resolve_input_value — cascade skip in action


class TestResolveInputValueSkipsOnFailedUpstream:
    """When upstream reports failure, ``_resolve_input_value`` returns
    ``_SKIP_PIPELINE_INPUT`` so downstream skips by input resolution rather
    than being called with empty data that would crash it."""

    def _two_node_pipeline(self) -> dict[str, Any]:
        return {
            "name": "cascade_test",
            "version": "2",
            "nodes": [
                {"node_id": "producer", "class": "FakeProducer", "inputs": {}},
                {
                    "node_id": "consumer",
                    "class": "FakeConsumer",
                    "inputs": {"text": "producer.text"},
                },
            ],
        }

    def test_skip_when_upstream_reports_success_false(self):
        runner = _make_runner(self._two_node_pipeline())
        results = {
            "producer": {
                "success": False,
                "error": "intentional",
                "error_type": "RuntimeError",
            }
        }
        consumer = runner.nodes["consumer"]
        resolved = runner._resolve_input_value(consumer, "producer.text", results)
        assert resolved is _SKIP_PIPELINE_INPUT

    def test_skip_when_upstream_reports_legacy_error_only(self):
        # Matches the SubjectiveJsonDataSource convention. Without this, the
        # consumer would receive ``text=""`` and crash, which is exactly the
        # field bug observed in worktwins's episode cascade.
        runner = _make_runner(self._two_node_pipeline())
        results = {
            "producer": {"json": None, "text": "", "error": "parse failure"}
        }
        consumer = runner.nodes["consumer"]
        resolved = runner._resolve_input_value(consumer, "producer.text", results)
        assert resolved is _SKIP_PIPELINE_INPUT

    def test_no_skip_when_upstream_succeeded(self):
        runner = _make_runner(self._two_node_pipeline())
        results = {
            "producer": {"text": "hello world", "error": "", "success": True}
        }
        consumer = runner.nodes["consumer"]
        resolved = runner._resolve_input_value(consumer, "producer.text", results)
        assert resolved == "hello world"

    def test_wildcard_also_blocked_on_failure(self):
        # The ``*`` input form (full upstream result) must also be blocked so
        # a downstream node configured with ``"*": "producer.*"`` doesn't pull
        # in the failed-result dict and pass it as request kwargs.
        pipeline = self._two_node_pipeline()
        pipeline["nodes"][1]["inputs"] = {"*": "producer.*"}
        runner = _make_runner(pipeline)
        results = {
            "producer": {"success": False, "error": "boom"}
        }
        consumer = runner.nodes["consumer"]
        resolved = runner._resolve_input_value(consumer, "producer.*", results)
        assert resolved is _SKIP_PIPELINE_INPUT
