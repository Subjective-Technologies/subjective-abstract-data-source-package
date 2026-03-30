from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DEPS_ROOT = ROOT.parent
CONFIG_PKG = DEPS_ROOT / "brainboost_configuration_package"
LOGGER_PKG = DEPS_ROOT / "brainboost_data_source_logger_package"
for pkg_path in (ROOT, CONFIG_PKG, LOGGER_PKG):
    if str(pkg_path) not in sys.path:
        sys.path.insert(0, str(pkg_path))

_log_lines: list[str] = []

if "brainboost_data_source_logger_package.BBLogger" not in sys.modules:
    logger_pkg = types.ModuleType("brainboost_data_source_logger_package")
    logger_mod = types.ModuleType("brainboost_data_source_logger_package.BBLogger")

    class BBLogger:
        @staticmethod
        def log(msg):
            _log_lines.append(str(msg))

    logger_mod.BBLogger = BBLogger
    logger_pkg.BBLogger = BBLogger
    sys.modules["brainboost_data_source_logger_package"] = logger_pkg
    sys.modules["brainboost_data_source_logger_package.BBLogger"] = logger_mod
else:
    from brainboost_data_source_logger_package.BBLogger import BBLogger

    _original_log = BBLogger.log

    @staticmethod
    def _capturing_log(msg):
        _log_lines.append(str(msg))
        _original_log(msg)

    BBLogger.log = _capturing_log

from subjective_abstract_data_source_package.SubjectiveDataSource import SubjectiveDataSource

pipeline_module = importlib.import_module(
    "subjective_abstract_data_source_package.SubjectivePipelineDataSource"
)


class FakeNoResponseDS(SubjectiveDataSource):
    run_calls = 0

    @classmethod
    def connection_schema(cls):
        return {}

    def run(self, request):
        self.__class__.run_calls += 1
        return {}


class FakeTextDS(SubjectiveDataSource):
    run_calls = 0
    received_requests: list[dict] = []

    @classmethod
    def connection_schema(cls):
        return {"text": {"type": "text"}}

    @classmethod
    def output_schema(cls):
        return {"text": {"type": "text"}}

    def run(self, request):
        self.__class__.run_calls += 1
        self.__class__.received_requests.append(dict(request or {}))
        return {"text": str((request or {}).get("text") or "")}


@pytest.fixture(autouse=True)
def _reset_state():
    _log_lines.clear()
    FakeNoResponseDS.run_calls = 0
    FakeTextDS.run_calls = 0
    FakeTextDS.received_requests.clear()
    yield


@pytest.fixture
def run_pipeline(monkeypatch, tmp_path):
    class_map = {
        "FakeNoResponseDS": FakeNoResponseDS,
        "SubjectiveTextDataSource": FakeTextDS,
    }

    monkeypatch.setattr(
        pipeline_module,
        "import_datasource_class",
        lambda class_name, project_root=None: class_map[class_name],
    )
    monkeypatch.setattr(
        pipeline_module._SubjectiveDataSourcePipelineRunner,
        "_load_connection_records",
        lambda self: {
            "source_conn": {"connection_data": {}},
            "prompt_conn": {"connection_data": {}},
        },
    )

    def _run(config: dict):
        context_dir = tmp_path / "context"
        context_dir.mkdir(exist_ok=True)
        datasource = pipeline_module.SubjectivePipelineDataSource(
            connection={"pipeline_json": config},
            config={"output_dir": str(context_dir), "context_dir": str(context_dir)},
        )
        return datasource.run({})

    return _run


def test_text_node_with_unresolved_wildcard_reference_is_skipped(run_pipeline):
    config = {
        "name": "wildcard_skip_pipeline",
        "version": "2",
        "nodes": [
            {
                "node_id": "source",
                "class": "FakeNoResponseDS",
                "connection_name": "source_conn",
                "inputs": {},
                "send_to_context": False,
            },
            {
                "node_id": "prompt",
                "class": "SubjectiveTextDataSource",
                "connection_name": "prompt_conn",
                "inputs": {
                    "*": "source.response",
                    "text": "PROMPT",
                },
                "send_to_context": True,
            },
        ],
        "workbench": {"nodes": [], "connections": [{"from_node": "source", "to_node": "prompt"}]},
    }

    result = run_pipeline(config)

    assert FakeNoResponseDS.run_calls == 1
    assert FakeTextDS.run_calls == 0
    assert result["pipeline_result"] == {}
    assert any("prompt skipped by input resolution" in line for line in _log_lines)
    assert any("unresolved reference for input '*'" in line for line in _log_lines)


def test_text_node_without_upstream_wildcard_reference_still_runs(run_pipeline):
    config = {
        "name": "static_text_pipeline",
        "version": "2",
        "nodes": [
            {
                "node_id": "prompt",
                "class": "SubjectiveTextDataSource",
                "connection_name": "prompt_conn",
                "inputs": {
                    "text": "PROMPT",
                },
                "send_to_context": True,
            }
        ],
        "workbench": {"nodes": [], "connections": []},
    }

    result = run_pipeline(config)

    assert FakeTextDS.run_calls == 1
    assert FakeTextDS.received_requests == [{"text": "PROMPT"}]
    assert result["pipeline_result"] == {"text": "PROMPT"}
