"""
SubjectivePipelineDataSource

Legacy programmatic pipeline runner plus the v2 pipeline wrapper used by the
desktop app. The direct runner remains available for backward compatibility.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import copy
import importlib
import inspect
import json
import os
import re
import shutil
import sys as _sys
import tempfile
import threading
import time
import traceback
import uuid
from typing import Any, Callable, Dict, List, Optional

from brainboost_data_source_logger_package.BBLogger import BBLogger

from subjective_abstract_data_source_package import SubjectiveDataSource
from subjective_abstract_data_source_package.datasource_importer import import_datasource_class
from subjective_abstract_data_source_package.pipeline_ticker_config import (
    normalize_pipeline_ticker_config,
    validate_pipeline_ticker_config,
)
from subjective_abstract_data_source_package.pipeline_accumulator_config import (
    normalize_pipeline_accumulator_config,
)


PIPELINE_TICKER_UNIT_TO_SECONDS = {
    "ms": 0.001,
    "sec": 1.0,
    "min": 60.0,
    "hour": 3600.0,
}


def _inline_ticker_wait_seconds(ticker_config: Dict[str, Any], cron_iter=None) -> float:
    if str(ticker_config.get("mode") or "") == "cron":
        if cron_iter is None:
            return 60.0
        next_time = cron_iter.get_next(datetime)
        return max(0.1, (next_time - datetime.now()).total_seconds())
    unit = str(ticker_config.get("interval_unit") or "sec").strip().lower()
    value = int(ticker_config.get("interval_value") or 0)
    multiplier = PIPELINE_TICKER_UNIT_TO_SECONDS.get(unit, 1.0)
    return max(0.001, float(value) * float(multiplier))


def _inline_ticker_stream_payload(
    pipeline_name: str,
    ticker_config: Dict[str, Any],
    tick_number: int,
    start_time: float,
    immediate: bool = False,
) -> Dict[str, Any]:
    """Single stream event dict for __ticker__ workflow nodes (matches workbench output ports)."""
    tick_data = {
        "tick": True,
        "timestamp": time.time(),
        "timestamp_iso": datetime.now().isoformat(),
        "datasource_id": pipeline_name,
        "datasource_type": "SubjectivePipelineDataSource",
        "mode": str(ticker_config.get("mode") or "interval"),
        "tick_number": int(tick_number),
        "elapsed_time": max(0.0, time.time() - float(start_time)),
        "subscriber": "",
    }
    if str(ticker_config.get("mode") or "") == "cron":
        tick_data["cron_expression"] = str(ticker_config.get("cron_expression") or "")
    else:
        tick_data["interval_value"] = int(ticker_config.get("interval_value") or 0)
        tick_data["interval_unit"] = str(ticker_config.get("interval_unit") or "sec")
    if immediate:
        tick_data["immediate"] = True
    out = dict(tick_data)
    out["whole output"] = dict(tick_data)
    return out


class InlineTickerWorkflowSource:
    """Inline v2 streaming source for workflow element __ticker__ (no external plugin)."""

    @classmethod
    def api_version(cls):
        return "v2"

    def __init__(self, connection=None, config=None):
        self._connection = connection or {}
        self._cfg = dict(config or {})
        merged = dict(self._cfg.get("internal_data") or {})
        merged["enabled"] = True
        self._ticker = normalize_pipeline_ticker_config(merged)
        self._stop_event = self._cfg.get("pipeline_stop_event")
        self._started_check = self._cfg.get("pipeline_started_check") or (lambda: True)
        self._pipeline_name = str(self._cfg.get("pipeline_name") or "pipeline")

    def supports_streaming(self):
        return True

    def stream(self, request):
        errs = validate_pipeline_ticker_config(self._ticker)
        if errs:
            raise ValueError("; ".join(errs))

        cron_iter = None
        if str(self._ticker.get("mode") or "") == "cron":
            from croniter import croniter

            cron_iter = croniter(str(self._ticker.get("cron_expression") or ""), datetime.now())

        start_time = time.time()
        tick_number = 0

        def _wait(seconds: float) -> bool:
            if self._stop_event is None:
                time.sleep(seconds)
                return False
            return bool(self._stop_event.wait(timeout=seconds))

        if self._ticker.get("immediate_first_tick"):
            tick_number += 1
            payload = _inline_ticker_stream_payload(
                self._pipeline_name,
                self._ticker,
                tick_number=tick_number,
                start_time=start_time,
                immediate=True,
            )
            BBLogger.log(
                f"[Pipeline:ticker] Tick #{tick_number} (immediate) from inline __ticker__ node"
            )
            yield payload

        while self._started_check() and (
            self._stop_event is None or not self._stop_event.is_set()
        ):
            if str(self._ticker.get("mode") or "") == "cron":
                from croniter import croniter

                cron_iter = croniter(
                    str(self._ticker.get("cron_expression") or ""),
                    datetime.now(),
                )
            wait_seconds = _inline_ticker_wait_seconds(self._ticker, cron_iter=cron_iter)
            if _wait(wait_seconds):
                break
            if not self._started_check():
                break
            tick_number += 1
            payload = _inline_ticker_stream_payload(
                self._pipeline_name,
                self._ticker,
                tick_number=tick_number,
                start_time=start_time,
                immediate=False,
            )
            BBLogger.log(
                f"[Pipeline:ticker] Tick #{tick_number} from inline __ticker__ node"
            )
            yield payload


@dataclass
class PipelineNode:
    node_id: str
    data_source_class: Any
    params: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    transform_fn: Optional[Callable] = None
    filter_fn: Optional[Callable] = None
    instance: Any = None

    def should_process(self, data: Any) -> bool:
        if self.filter_fn is None:
            return True
        try:
            return bool(self.filter_fn(data))
        except Exception as exc:
            BBLogger.log(f"Filter function error in node {self.node_id}: {exc}")
            return False

    def transform_data(self, data: Any) -> Any:
        if self.transform_fn is None:
            return data
        try:
            return self.transform_fn(data)
        except Exception as exc:
            BBLogger.log(f"Transform function error in node {self.node_id}: {exc}")
            return data


class PipelineAdapter:
    def __init__(self, pipeline_node: PipelineNode, pipeline: "SubjectiveDataSourcePipeline"):
        self.pipeline_node = pipeline_node
        self.pipeline = pipeline

    def update(self, data: Any):
        if not self.pipeline_node.should_process(data):
            return
        self.pipeline._trigger_node(
            self.pipeline_node.node_id,
            self.pipeline_node.transform_data(data),
        )

    def notify(self, data: Any):
        self.update(data)


class SubjectiveDataSourcePipeline(SubjectiveDataSource):
    """
    Legacy in-memory pipeline runner used by existing tests and older `.pipe` files.
    """

    def __init__(
        self,
        name: str = None,
        session=None,
        dependency_data_sources: List[SubjectiveDataSource] = None,
        subscribers=None,
        params: Dict[str, Any] = None,
    ):
        super().__init__(
            name=name,
            session=session,
            dependency_data_sources=dependency_data_sources or [],
            subscribers=subscribers,
            params=params or {},
        )
        self.nodes: Dict[str, PipelineNode] = {}
        self.adapters: Dict[str, List[PipelineAdapter]] = {}
        self._started = False
        self._pipeline_config = None
        self._pipeline_file_path = None

        pipeline_file = self.params.get("pipeline_file")
        if pipeline_file:
            self._pipeline_file_path = pipeline_file
            self._load_from_pipe_file(pipeline_file)

    def _load_from_pipe_file(self, file_path: str):
        with open(file_path, "r", encoding="utf-8") as fh:
            self._pipeline_config = json.load(fh)
        self._build_from_config()

    def _build_from_config(self):
        if not self._pipeline_config:
            raise ValueError("No pipeline configuration loaded")
        nodes = self._pipeline_config.get("nodes")
        if not isinstance(nodes, list):
            raise ValueError("Pipeline config must have a 'nodes' list")

        if self._pipeline_config.get("name") and not self.name:
            self.name = self._pipeline_config["name"]

        workbench_connections = {}
        workbench = self._pipeline_config.get("workbench", {}) or {}
        for conn in workbench.get("connections", []):
            if not isinstance(conn, dict):
                continue
            workbench_connections.setdefault(str(conn.get("to_node") or ""), []).append(
                str(conn.get("from_node") or "")
            )

        for node_config in nodes:
            node_id = str(node_config.get("node_id") or "").strip()
            if not node_id:
                raise ValueError("Each node must have a 'node_id'")

            class_name = str(node_config.get("class") or "").strip()
            module_path = str(node_config.get("module") or "").strip()
            if not class_name:
                raise ValueError(f"Node '{node_id}' must specify 'class'")

            if module_path:
                try:
                    module = importlib.import_module(module_path)
                    data_source_class = getattr(module, class_name)
                except Exception:
                    data_source_class = import_datasource_class(class_name, project_root=os.getcwd())
            else:
                data_source_class = import_datasource_class(class_name, project_root=os.getcwd())

            params = node_config.get("params", {}) or {}
            dependencies = node_config.get("dependencies", []) or []

            if not dependencies and str(self._pipeline_config.get("version") or "") == "2":
                inputs = node_config.get("inputs", {}) or {}
                dependencies = self._extract_dependencies_from_inputs(inputs)
                if not dependencies:
                    dependencies = workbench_connections.get(node_id, [])

            filter_fn = None
            transform_fn = None
            if node_config.get("filter"):
                filter_fn = self._create_filter_function(str(node_config["filter"]))
            if node_config.get("transform"):
                transform_fn = self._create_transform_function(str(node_config["transform"]))

            self.add_node(
                node_id=node_id,
                data_source_class=data_source_class,
                params=params,
                dependencies=list(dependencies),
                transform_fn=transform_fn,
                filter_fn=filter_fn,
            )

    def _extract_dependencies_from_inputs(self, inputs: Dict[str, Any]) -> List[str]:
        deps = []
        if not isinstance(inputs, dict):
            return deps
        for value in inputs.values():
            if isinstance(value, str) and "." in value and not value.startswith("$"):
                dep_id = value.split(".", 1)[0].strip()
                if dep_id and dep_id not in deps:
                    deps.append(dep_id)
        return deps

    def _create_filter_function(self, filter_expr: str):
        def filter_fn(data):
            try:
                return eval(
                    filter_expr,
                    {"__builtins__": {}},
                    {"data": data, "str": str, "int": int, "float": float},
                )
            except Exception as exc:
                BBLogger.log(f"Error evaluating filter expression '{filter_expr}': {exc}")
                return False

        return filter_fn

    def _create_transform_function(self, transform_expr: str):
        def transform_fn(data):
            try:
                return eval(
                    transform_expr,
                    {"__builtins__": {}},
                    {"data": data, "str": str, "int": int, "float": float},
                )
            except Exception as exc:
                BBLogger.log(f"Error evaluating transform expression '{transform_expr}': {exc}")
                return data

        return transform_fn

    def add_node(
        self,
        node_id: str,
        data_source_class,
        params: Optional[Dict[str, Any]] = None,
        dependencies: Optional[List[str]] = None,
        transform_fn: Optional[Callable] = None,
        filter_fn: Optional[Callable] = None,
    ) -> "SubjectiveDataSourcePipeline":
        if node_id in self.nodes:
            raise ValueError(f"Node with id '{node_id}' already exists in pipeline")
        self.nodes[node_id] = PipelineNode(
            node_id=node_id,
            data_source_class=data_source_class,
            params=params or {},
            dependencies=list(dependencies or []),
            transform_fn=transform_fn,
            filter_fn=filter_fn,
        )
        return self

    def add_node_from_module(
        self,
        node_id: str,
        module_path: str,
        class_name: str,
        params: Optional[Dict[str, Any]] = None,
        dependencies: Optional[List[str]] = None,
        transform_fn: Optional[Callable] = None,
        filter_fn: Optional[Callable] = None,
    ) -> "SubjectiveDataSourcePipeline":
        module = importlib.import_module(module_path)
        data_source_class = getattr(module, class_name)
        return self.add_node(
            node_id=node_id,
            data_source_class=data_source_class,
            params=params,
            dependencies=dependencies,
            transform_fn=transform_fn,
            filter_fn=filter_fn,
        )

    def _validate_dependencies(self):
        for node_id, node in self.nodes.items():
            for dep_id in node.dependencies:
                if dep_id not in self.nodes:
                    raise ValueError(f"Node '{node_id}' depends on non-existent node '{dep_id}'")

        visited = set()
        rec_stack = set()

        def has_cycle(node_id):
            visited.add(node_id)
            rec_stack.add(node_id)
            for dep_id in self.nodes[node_id].dependencies:
                if dep_id not in visited:
                    if has_cycle(dep_id):
                        return True
                elif dep_id in rec_stack:
                    return True
            rec_stack.remove(node_id)
            return False

        for node_id in self.nodes:
            if node_id not in visited and has_cycle(node_id):
                raise ValueError(f"Circular dependency detected involving node '{node_id}'")

    def build(self):
        if self._started:
            return

        self._validate_dependencies()
        for node in self.nodes.values():
            node.instance = self._instantiate_legacy_node(node)

        for node_id, node in self.nodes.items():
            for dep_id in node.dependencies:
                adapter = PipelineAdapter(node, self)
                dep_instance = self.nodes[dep_id].instance
                if hasattr(dep_instance, "subscribe"):
                    dep_instance.subscribe(adapter)
                self.adapters.setdefault(node_id, []).append(adapter)

        self._started = True

    def _instantiate_legacy_node(self, node: PipelineNode):
        kwargs = {
            "name": node.node_id,
            "dependency_data_sources": [
                self.nodes[dep_id].instance
                for dep_id in node.dependencies
                if dep_id in self.nodes and self.nodes[dep_id].instance is not None
            ],
            "params": dict(node.params or {}),
        }
        try:
            signature = inspect.signature(node.data_source_class.__init__)
            accepts_var_kw = any(
                parameter.kind == parameter.VAR_KEYWORD
                for parameter in signature.parameters.values()
            )
            if not accepts_var_kw:
                kwargs = {
                    key: value
                    for key, value in kwargs.items()
                    if key in signature.parameters
                }
        except Exception:
            pass
        return node.data_source_class(**kwargs) if kwargs else node.data_source_class()

    def start(self):
        if not self._started:
            self.build()

        started = set()

        def start_node(node_id):
            if node_id in started:
                return
            node = self.nodes[node_id]
            for dep_id in node.dependencies:
                start_node(dep_id)
            if not node.dependencies and hasattr(node.instance, "fetch"):
                thread = threading.Thread(
                    target=node.instance.fetch,
                    name=f"Pipeline-{self.get_name()}-{node_id}",
                    daemon=True,
                )
                thread.start()
            started.add(node_id)

        for node_id in self.nodes:
            start_node(node_id)

    def stop(self):
        for node in self.nodes.values():
            if node.instance and hasattr(node.instance, "stop"):
                try:
                    node.instance.stop()
                except Exception as exc:
                    BBLogger.log(f"Error stopping node '{node.node_id}': {exc}")
        self._started = False

    def _trigger_node(self, node_id: str, data: Any):
        node = self.nodes.get(node_id)
        if not node or not node.instance:
            return
        if not node.should_process(data):
            return
        data = node.transform_data(data)
        if hasattr(node.instance, "process_input"):
            node.instance.process_input(data)
        elif hasattr(node.instance, "update"):
            node.instance.update(data)

    def get_node_instance(self, node_id: str):
        node = self.nodes.get(node_id)
        return node.instance if node else None

    def fetch(self):
        self.start()
        while self._started:
            time.sleep(1)

    def get_icon(self) -> str:
        return SubjectivePipelineDataSource.icon()

    def get_connection_data(self) -> dict:
        return {
            "connection_type": "PIPELINE",
            "fields": [],
        }

    def handle_redis_payload(self, payload: dict):
        try:
            emitting_node_id = None
            for node_id, node in self.nodes.items():
                params = node.params or {}
                node_conn_name = params.get("connection_name")
                node_ds_name = params.get("datasource_name")
                if node_conn_name and payload.get("connection_name") == node_conn_name:
                    emitting_node_id = node_id
                    break
                if node_ds_name and payload.get("datasource_name") == node_ds_name:
                    emitting_node_id = node_id
                    break
                if payload.get("datasource_name") == node_id:
                    emitting_node_id = node_id
                    break

            if not emitting_node_id:
                return

            for node_id, node in self.nodes.items():
                if emitting_node_id in node.dependencies:
                    self._trigger_node(node_id, payload)
        except Exception as exc:
            BBLogger.log(f"Error processing Redis payload: {exc}")


@dataclass
class _V2PipelineNode:
    node_id: str
    class_name: str
    connection_name: str
    inputs: Dict[str, Any] = field(default_factory=dict)
    selected_action: str = ""
    terminal: bool = False
    dependencies: List[str] = field(default_factory=list)
    filter_expr: str = ""
    transform_expr: str = ""
    internal_data: Dict[str, Any] = field(default_factory=dict)
    data_source_class: Any = None
    instance: Any = None


_SKIP_PIPELINE_NODE = object()
_SKIP_PIPELINE_INPUT = object()


class _SubjectiveDataSourcePipelineRunner:
    def __init__(
        self,
        pipeline_name: str,
        pipeline_config: Dict[str, Any],
        context_dir: str = "",
        tmp_root: str = "",
    ):
        self.pipeline_name = pipeline_name or "pipeline"
        self.pipeline_config = pipeline_config or {}
        self.context_dir = context_dir or tempfile.mkdtemp(prefix="subjective_pipeline_context_")
        self.tmp_root = tmp_root or tempfile.gettempdir()
        self.workspace_root = os.path.join(
            self.tmp_root,
            "pipeline_runs",
            f"{self.pipeline_name}_{uuid.uuid4().hex[:8]}",
        )
        self.nodes: Dict[str, _V2PipelineNode] = {}
        self._connection_records = self._load_connection_records()
        self._started = False
        self._iterated_nodes: set[str] = set()
        self._workflow_start_node_ids: set[str] = set()
        self._workflow_end_node_ids: set[str] = set()
        self._workflow_iterator_nodes: Dict[str, Dict[str, Any]] = {}
        self._workflow_accumulator_nodes: Dict[str, Dict[str, Any]] = {}
        self._accumulator_buffers: Dict[str, List[Any]] = {}
        self._accumulator_last_markers: Dict[str, Dict[str, Any]] = {}
        self._node_result_versions: Dict[str, int] = {}
        self._stream_threads: List[threading.Thread] = []
        self._stop_event = threading.Event()
        self._build_nodes()

    def _crash_log(self, msg):
        """Write directly to a crash log file, bypassing BBLogger's async queue."""
        try:
            log_dir = os.path.dirname(BBLogger._get_log_file_path() or "")
            if not log_dir:
                log_dir = os.path.expanduser("~")
            crash_path = os.path.join(log_dir, "pipeline_crash_debug.log")
            with open(crash_path, "a", encoding="utf-8") as f:
                from datetime import datetime
                f.write(f"{datetime.now().isoformat()} | {msg}\n")
                f.flush()
        except Exception:
            pass

    def build(self):
        self._crash_log(f"build() entered. workspace={self.workspace_root}")
        BBLogger.log(f"[Pipeline:build] Creating workspace at {self.workspace_root}")
        os.makedirs(self.workspace_root, exist_ok=True)
        for node in self.nodes.values():
            self._ensure_node_dirs(node.node_id)
            if node.instance is None:
                class_name = getattr(node.data_source_class, "__name__", str(node.data_source_class))
                try:
                    self._crash_log(f"Instantiating {node.node_id} ({class_name})")
                    BBLogger.log(f"[Pipeline:build] Instantiating {node.node_id} ({class_name})")
                    node.instance = self._instantiate_node(node)
                    self._crash_log(f"{node.node_id} instantiated OK")
                    BBLogger.log(f"[Pipeline:build] {node.node_id} instantiated OK")
                except Exception as exc:
                    msg = (
                        f"FAILED to instantiate {node.node_id} ({class_name}): "
                        f"{exc}\n{traceback.format_exc()}"
                    )
                    self._crash_log(msg)
                    BBLogger.log(f"[Pipeline:build] {msg}")
                    raise
        self._started = True
        self._stop_event.clear()
        self._crash_log(f"All {len(self.nodes)} nodes instantiated. Build complete.")
        BBLogger.log(f"[Pipeline:build] All {len(self.nodes)} nodes instantiated. Build complete.")
        return self

    def start(self, request: Optional[Dict[str, Any]] = None):
        request_payload = dict(request or {})
        BBLogger.log(
            f"[Pipeline:start] Starting pipeline '{self.pipeline_name}' "
            f"with request keys={sorted(request_payload.keys())}"
        )
        return self.run(request_payload)

    def run(self, request: Optional[Dict[str, Any]] = None):
        if not self._started:
            BBLogger.log("[Pipeline:run] Runner not built yet. Calling build().")
            self.build()

        initial_request = dict(request or {})
        results: Dict[str, Any] = {}
        topological_order = self._topological_order()
        streaming_nodes: List[_V2PipelineNode] = []
        batch_order: List[str] = []
        for node_id in topological_order:
            node = self.nodes[node_id]
            if self._is_streaming_node(node):
                streaming_nodes.append(node)
            else:
                batch_order.append(node_id)

        BBLogger.log(f"[Pipeline:run] Topological order: {topological_order}")
        BBLogger.log(f"[Pipeline:run] Streaming nodes: {[node.node_id for node in streaming_nodes]}")
        BBLogger.log(f"[Pipeline:run] Batch nodes: {batch_order}")
        if not streaming_nodes:
            BBLogger.log("[Pipeline:run] No streaming nodes detected. Running batch-only mode.")
            return self._run_batch(initial_request, results, topological_order)
        BBLogger.log("[Pipeline:run] Streaming nodes detected. Running event-driven mode.")
        return self._run_event_driven(initial_request, results, streaming_nodes, batch_order)

    def stop(self):
        self._started = False
        self._stop_event.set()
        for node in self.nodes.values():
            if node.instance and hasattr(node.instance, "stop"):
                try:
                    node.instance.stop()
                except Exception as exc:
                    BBLogger.log(f"Error stopping pipeline node '{node.node_id}': {exc}")

    def get_node_instance(self, node_id: str):
        node = self.nodes.get(node_id)
        return node.instance if node else None

    def _run_batch(
        self,
        initial_request: Dict[str, Any],
        results: Dict[str, Any],
        node_order: Optional[List[str]] = None,
    ):
        self._iterated_nodes.clear()
        self._run_batch_nodes(node_order or self._topological_order(), initial_request, results)
        return self._finalize_results(results)

    def _run_batch_nodes(
        self,
        node_order: List[str],
        initial_request: Dict[str, Any],
        results: Dict[str, Any],
        reset_workflow_cycle: bool = True,
    ) -> None:
        if reset_workflow_cycle:
            self._clear_accumulator_cycle_outputs(results)
        for node_id in node_order:
            node = self.nodes[node_id]
            if not all(dep_id in results for dep_id in node.dependencies):
                missing = [dep_id for dep_id in node.dependencies if dep_id not in results]
                BBLogger.log(
                    f"[Pipeline:batch] Skipping {node_id}; waiting on dependencies: {missing}"
                )
                continue

            node_request, iteration_specs = self._build_request_for_node(node, results, initial_request)
            if node_request is _SKIP_PIPELINE_NODE:
                self._iterated_nodes.discard(node_id)
                results[node_id] = {}
                BBLogger.log(f"[Pipeline:batch] {node_id} skipped by input resolution.")
                continue

            execution_plans = self._expand_iteration_requests(node, node_request, iteration_specs)
            if not execution_plans:
                if not iteration_specs:
                    self._iterated_nodes.discard(node_id)
                results[node_id] = [] if iteration_specs else {}
                BBLogger.log(
                    f"[Pipeline:batch] {node_id} produced no execution plans. "
                    f"iteration_specs={len(iteration_specs)}"
                )
                continue

            execution_results: List[Any] = []
            BBLogger.log(
                f"[Pipeline:batch] Executing {node_id} with {len(execution_plans)} plan(s). "
                f"iteration_specs={len(iteration_specs)}"
            )
            for execution_plan in execution_plans:
                prepared_request = self._prepare_request_for_execution(
                    node,
                    execution_plan.get("request", {}),
                    input_dir_override=execution_plan.get("input_dir"),
                )
                result = self._execute_node(
                    node,
                    prepared_request,
                    input_dir_override=execution_plan.get("input_dir"),
                )
                execution_results.append(result)
                self._route_output_files(node)

            if iteration_specs:
                results[node_id] = self._aggregate_iteration_results(execution_results)
                self._iterated_nodes.add(node_id)
            else:
                self._iterated_nodes.discard(node_id)
                results[node_id] = execution_results[-1] if execution_results else {}
            self._bump_result_version(node_id)
            self._refresh_workflow_accumulators(results, changed_node_id=node_id)

    def _finalize_results(self, results: Dict[str, Any]):
        terminal_nodes = [n.node_id for n in self.nodes.values() if n.terminal]
        if not terminal_nodes and self.nodes:
            terminal_nodes = [self._topological_order()[-1]]
        if len(terminal_nodes) == 1:
            return results.get(terminal_nodes[0])
        return {node_id: results.get(node_id) for node_id in terminal_nodes}

    def _is_v2_node(self, node: _V2PipelineNode) -> bool:
        api_version = getattr(node.data_source_class, "api_version", None)
        return callable(api_version) and node.data_source_class.api_version() == "v2"

    def _is_streaming_node(self, node: _V2PipelineNode) -> bool:
        if str(node.class_name or "").strip() == "__ticker__":
            return True
        if node.instance is None:
            node.instance = self._instantiate_node(node)
        if hasattr(node.instance, "supports_streaming"):
            try:
                if bool(node.instance.supports_streaming()):
                    return True
            except Exception as exc:
                BBLogger.log(f"Error detecting streaming support for '{node.node_id}': {exc}")
        return bool(not self._is_v2_node(node) and not node.dependencies and hasattr(node.instance, "fetch"))

    def _build_request_for_streaming_node(
        self,
        node: _V2PipelineNode,
        initial_request: Dict[str, Any],
    ) -> Dict[str, Any]:
        request: Dict[str, Any] = {}
        if node.inputs:
            for key, raw_value in node.inputs.items():
                resolved = self._resolve_input_value(node, raw_value, {})
                if resolved in (_SKIP_PIPELINE_NODE, _SKIP_PIPELINE_INPUT):
                    continue
                if key in ("*", "$default") and isinstance(resolved, dict):
                    request.update(resolved)
                elif key in ("*", "$default"):
                    request["value"] = resolved
                else:
                    request[key] = resolved
        for key, value in initial_request.items():
            request.setdefault(key, value)
        return request

    def _dependency_closure(self) -> Dict[str, set[str]]:
        cache: Dict[str, set[str]] = {}

        def _collect(node_id: str) -> set[str]:
            if node_id in cache:
                return cache[node_id]
            deps = set(self.nodes[node_id].dependencies)
            for dep_id in list(deps):
                if dep_id in self.nodes:
                    deps.update(_collect(dep_id))
            cache[node_id] = deps
            return deps

        for node_id in self.nodes:
            _collect(node_id)
        return cache

    def _bump_result_version(self, node_id: str) -> int:
        node_id = str(node_id or "").strip()
        if not node_id:
            return 0
        version = int(self._node_result_versions.get(node_id, 0) or 0) + 1
        self._node_result_versions[node_id] = version
        return version

    def _clear_accumulator_cycle_outputs(self, results: Dict[str, Any]) -> None:
        for accumulator_node_id in self._workflow_accumulator_nodes:
            results.pop(accumulator_node_id, None)

    def _iter_accumulator_raw_values(self, raw_inputs: Dict[str, Any]) -> List[tuple[str, int, Any]]:
        values: List[tuple[str, int, Any]] = []
        for port_name, raw_value in (raw_inputs or {}).items():
            if isinstance(raw_value, list):
                for index, candidate in enumerate(raw_value):
                    values.append((str(port_name or ""), index, candidate))
            else:
                values.append((str(port_name or ""), 0, raw_value))
        return values

    def _accumulator_reference_marker(self, raw_value: Any, results: Dict[str, Any]) -> Any:
        if isinstance(raw_value, dict) and "from" in raw_value:
            raw_value = raw_value.get("from")
        if not isinstance(raw_value, str):
            return None
        source_ref = str(raw_value or "").strip()
        if "." not in source_ref or source_ref.startswith("$"):
            return None

        source_node_id, field_name = source_ref.split(".", 1)
        source_node_id = source_node_id.strip()
        field_name = field_name.strip()

        if source_node_id in self.nodes:
            version = int(self._node_result_versions.get(source_node_id, 0) or 0)
            return (source_node_id, version, field_name) if version > 0 else None
        if source_node_id in self._workflow_accumulator_nodes:
            version = int(self._node_result_versions.get(source_node_id, 0) or 0)
            return (source_node_id, version, field_name) if version > 0 and source_node_id in results else None
        if source_node_id in self._workflow_iterator_nodes:
            versions = tuple(
                (dep_id, int(self._node_result_versions.get(dep_id, 0) or 0))
                for dep_id in self._workflow_iterator_nodes[source_node_id].get("dependencies", [])
            )
            if not any(version > 0 for _, version in versions):
                return None
            return (source_node_id, versions, field_name)
        return None

    def _resolve_accumulator_buffer_value(self, raw_value: Any, results: Dict[str, Any]) -> Any:
        if isinstance(raw_value, dict) and "from" in raw_value:
            raw_value = raw_value.get("from")
        if not isinstance(raw_value, str):
            return None
        source_ref = str(raw_value or "").strip()
        if "." not in source_ref or source_ref.startswith("$"):
            return None

        source_node_id, field_name = source_ref.split(".", 1)
        source_node_id = source_node_id.strip()
        field_name = field_name.strip()

        if source_node_id in self._workflow_iterator_nodes:
            return self._resolve_iterator_output(source_node_id, field_name, results)
        if source_node_id in self._workflow_accumulator_nodes:
            return self._resolve_accumulator_output(
                source_node_id,
                field_name,
                results,
                skip_if_unready=False,
            )
        if source_node_id not in self.nodes:
            return None

        source_value = results.get(source_node_id)
        if source_value is None:
            return None
        if field_name == "*":
            return copy.deepcopy(source_value)
        if isinstance(source_value, dict):
            return copy.deepcopy(source_value.get(field_name))
        return None

    def _stringify_accumulator_item(self, value: Any) -> str:
        if isinstance(value, str):
            return value
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)

    def _maybe_release_accumulator(self, accumulator_node_id: str) -> Any:
        accumulator_node = self._workflow_accumulator_nodes.get(accumulator_node_id) or {}
        config = accumulator_node.get("config", {}) or {}
        buffer = self._accumulator_buffers.setdefault(accumulator_node_id, [])
        threshold = max(1, int(config.get("threshold") or 5))
        if len(buffer) < threshold:
            return None

        items = list(buffer)
        if str(accumulator_node.get("class_name") or "").strip() == "__accumulator_stack__":
            items.reverse()

        buffer.clear()
        release_mode = str(config.get("release_mode") or "array").strip().lower()
        if release_mode == "concatenated":
            separator = str(config.get("separator") if config.get("separator") is not None else "\n")
            return separator.join(self._stringify_accumulator_item(item) for item in items)
        return copy.deepcopy(items)

    def _refresh_workflow_accumulators(
        self,
        results: Dict[str, Any],
        changed_node_id: Optional[str] = None,
    ) -> None:
        for accumulator_node_id, accumulator_node in self._workflow_accumulator_nodes.items():
            dependencies = accumulator_node.get("dependencies", []) or []
            if changed_node_id and dependencies and changed_node_id not in dependencies:
                continue

            raw_inputs = accumulator_node.get("inputs", {}) or {}
            markers = self._accumulator_last_markers.setdefault(accumulator_node_id, {})
            pending_items: List[Any] = []
            for port_name, index, raw_value in self._iter_accumulator_raw_values(raw_inputs):
                ref_key = f"{port_name}:{index}"
                marker = self._accumulator_reference_marker(raw_value, results)
                if marker is None or markers.get(ref_key) == marker:
                    continue
                resolved = self._resolve_accumulator_buffer_value(raw_value, results)
                if resolved in (None, _SKIP_PIPELINE_INPUT, _SKIP_PIPELINE_NODE):
                    continue
                markers[ref_key] = marker
                pending_items.append(copy.deepcopy(resolved))

            if pending_items:
                self._accumulator_buffers.setdefault(accumulator_node_id, []).extend(pending_items)

            released_value = self._maybe_release_accumulator(accumulator_node_id)
            if released_value is None:
                continue

            results[accumulator_node_id] = {"output": released_value}
            self._bump_result_version(accumulator_node_id)
            BBLogger.log(
                f"[Pipeline:accumulator] Released {accumulator_node_id} "
                f"count={len(pending_items)} threshold="
                f"{accumulator_node.get('config', {}).get('threshold', 5)} "
                f"mode={accumulator_node.get('config', {}).get('release_mode', 'array')}"
            )

    def _run_event_driven(
        self,
        initial_request: Dict[str, Any],
        results: Dict[str, Any],
        streaming_nodes: List[_V2PipelineNode],
        batch_order: List[str],
    ):
        import queue

        event_queue: "queue.Queue[tuple[str, str, Any]]" = queue.Queue()
        dependency_map = self._dependency_closure()
        streaming_node_ids = {node.node_id for node in streaming_nodes}
        static_batch_order = [
            node_id
            for node_id in batch_order
            if dependency_map.get(node_id, set()).isdisjoint(streaming_node_ids)
        ]
        if static_batch_order:
            BBLogger.log(f"[Pipeline:event] Running static batch nodes first: {static_batch_order}")
            self._run_batch_nodes(static_batch_order, initial_request, results)

        self._stream_threads = []

        def _publish_stream_event(node: _V2PipelineNode, item: Any) -> None:
            self._write_result_envelope(node, item)
            self._route_output_files(node)
            summary = list(item.keys()) if isinstance(item, dict) else type(item).__name__
            BBLogger.log(f"[Pipeline:event] Published stream event from {node.node_id}: {summary}")
            event_queue.put(("event", node.node_id, item))

        def _stream_worker(node: _V2PipelineNode, stream_request: Dict[str, Any]) -> None:
            emitted_any = False
            try:
                if node.instance is None:
                    node.instance = self._instantiate_node(node)

                if self._is_v2_node(node) and node.instance.supports_streaming():
                    for item in node.instance.stream(stream_request):
                        if not self._started:
                            break
                        emitted_any = True
                        _publish_stream_event(node, item)
                else:
                    class _EventCollector:
                        def notify(self_c, data):
                            nonlocal emitted_any
                            if not self._started:
                                return
                            emitted_any = True
                            _publish_stream_event(node, data)

                        def update(self_c, data):
                            self_c.notify(data)

                    try:
                        node.instance.subscribe(_EventCollector())
                    except Exception:
                        pass

                    params = getattr(node.instance, "params", {})
                    if isinstance(params, dict):
                        runtime_config = self._runtime_config(node)
                        params.update(stream_request)
                        params["TARGET_DIRECTORY"] = runtime_config["output_dir"]
                        params["context_dir"] = runtime_config["output_dir"]

                    result = node.instance.fetch()
                    if result is not None and not emitted_any and self._started:
                        _publish_stream_event(node, result)
            except Exception as exc:
                BBLogger.log(f"Streaming node '{node.node_id}' error: {exc}")
            finally:
                event_queue.put(("done", node.node_id, None))

        for node in streaming_nodes:
            node_request = self._build_request_for_streaming_node(node, initial_request)
            prepared_request = self._prepare_request_for_execution(node, node_request)
            BBLogger.log(
                f"[Pipeline:event] Starting stream worker for {node.node_id} "
                f"with keys={sorted(prepared_request.keys())}"
            )
            thread = threading.Thread(
                target=_stream_worker,
                args=(node, prepared_request),
                name=f"Pipeline-{self.pipeline_name}-{node.node_id}",
                daemon=True,
            )
            self._stream_threads.append(thread)
            thread.start()

        completed_streams: set[str] = set()
        while self._started:
            try:
                timeout = 0.1 if len(completed_streams) == len(streaming_nodes) else 1.0
                event_type, source_node_id, payload = event_queue.get(timeout=timeout)
            except queue.Empty:
                if len(completed_streams) == len(streaming_nodes):
                    break
                continue

            if event_type == "done":
                completed_streams.add(source_node_id)
                BBLogger.log(
                    f"[Pipeline:event] Stream worker finished for {source_node_id}. "
                    f"completed={sorted(completed_streams)}"
                )
                non_ticker_streaming_ids = {
                    n.node_id
                    for n in streaming_nodes
                    if str(n.class_name or "").strip() != "__ticker__"
                }
                ticker_streaming_ids = {
                    n.node_id
                    for n in streaming_nodes
                    if str(n.class_name or "").strip() == "__ticker__"
                }
                if (
                    ticker_streaming_ids
                    and non_ticker_streaming_ids
                    and non_ticker_streaming_ids.issubset(completed_streams)
                ):
                    self._stop_event.set()
                continue

            if event_type != "event":
                continue

            self._clear_accumulator_cycle_outputs(results)
            results[source_node_id] = payload
            self._bump_result_version(source_node_id)
            self._refresh_workflow_accumulators(results, changed_node_id=source_node_id)
            affected_batch_nodes = [
                node_id
                for node_id in batch_order
                if source_node_id in dependency_map.get(node_id, set())
            ]
            if affected_batch_nodes:
                BBLogger.log(
                    f"[Pipeline:event] {source_node_id} triggered downstream batch nodes: {affected_batch_nodes}"
                )
                self._run_batch_nodes(
                    affected_batch_nodes,
                    initial_request,
                    results,
                    reset_workflow_cycle=False,
                )

        return self._finalize_results(results)

    def _load_connection_records(self) -> Dict[str, Dict[str, Any]]:
        try:
            from com_subjective_session.ConnectionPersistor import ConnectionPersistor

            persistor = ConnectionPersistor()
            records = {}
            for item in persistor.load_connections():
                if not isinstance(item, dict):
                    continue
                name = str(item.get("connection_name") or "").strip()
                if name:
                    records[name] = item
            return records
        except Exception as exc:
            BBLogger.log(f"Pipeline runner could not load persisted connections: {exc}")
            return {}

    def _build_nodes(self):
        nodes = self.pipeline_config.get("nodes", [])
        if not isinstance(nodes, list):
            raise ValueError("Pipeline config must contain a 'nodes' list")
        known_node_ids = {
            str(raw_node.get("node_id") or "").strip()
            for raw_node in nodes
            if isinstance(raw_node, dict) and str(raw_node.get("node_id") or "").strip()
        }

        workbench_inputs = {}
        workbench = self.pipeline_config.get("workbench", {}) or {}
        for conn in workbench.get("connections", []):
            if not isinstance(conn, dict):
                continue
            workbench_inputs.setdefault(str(conn.get("to_node") or ""), []).append(
                str(conn.get("from_node") or "")
            )

        terminal_source_ids: set[str] = set()
        # Identify workflow element node IDs so we can skip them and/or
        # interpret them at runtime.
        workflow_node_ids = set()
        for raw_node in nodes:
            if not isinstance(raw_node, dict):
                continue
            cls = str(raw_node.get("class") or "").strip()
            nid = str(raw_node.get("node_id") or "").strip()
            if cls.startswith("__") and cls.endswith("__") and nid:
                if cls != "__ticker__":
                    workflow_node_ids.add(nid)
                raw_inputs = raw_node.get("inputs", {}) or {}
                if not isinstance(raw_inputs, dict):
                    raw_inputs = {}
                if cls == "__start__":
                    self._workflow_start_node_ids.add(nid)
                elif cls == "__end__":
                    self._workflow_end_node_ids.add(nid)
                    terminal_source_ids.update(
                        self._extract_dependencies(raw_inputs, known_node_ids=known_node_ids)
                    )
                elif cls == "__iterator__":
                    self._workflow_iterator_nodes[nid] = {
                        "inputs": raw_inputs,
                        "dependencies": self._extract_dependencies(raw_inputs, known_node_ids=known_node_ids),
                    }
                elif cls in {"__accumulator_stack__", "__accumulator_queue__"}:
                    self._workflow_accumulator_nodes[nid] = {
                        "class_name": cls,
                        "inputs": raw_inputs,
                        "dependencies": self._extract_dependencies(raw_inputs, known_node_ids=known_node_ids),
                        "config": normalize_pipeline_accumulator_config(
                            raw_node.get("internal_data") if isinstance(raw_node.get("internal_data"), dict) else {}
                        ),
                    }

        for workflow_node in self._workflow_iterator_nodes.values():
            workflow_node["dependencies"] = self._expand_workflow_dependencies(
                workflow_node.get("dependencies", []),
                workflow_node_ids,
            )
        for workflow_node in self._workflow_accumulator_nodes.values():
            workflow_node["dependencies"] = self._expand_workflow_dependencies(
                workflow_node.get("dependencies", []),
                workflow_node_ids,
            )

        for raw_node in nodes:
            if not isinstance(raw_node, dict):
                continue
            node_id = str(raw_node.get("node_id") or "").strip()
            class_name = str(raw_node.get("class") or "").strip()
            if not node_id or not class_name:
                raise ValueError("Each v2 pipeline node must define 'node_id' and 'class'")

            # Workflow elements (e.g. __iterator__, __start__, __end__, __condition__,
            # __fork__, __function__) are visual constructs handled by the editor;
            # they have no backing datasource class to import. __ticker__ is special:
            # it runs as InlineTickerWorkflowSource and participates in event-driven mode.
            if class_name.startswith("__") and class_name.endswith("__"):
                if class_name != "__ticker__":
                    continue

            inputs = raw_node.get("inputs", {}) or {}
            if not isinstance(inputs, dict):
                inputs = {}
            raw_internal_data = raw_node.get("internal_data")
            internal_data = raw_internal_data if isinstance(raw_internal_data, dict) else {}
            dependencies = self._extract_dependencies(inputs, known_node_ids=known_node_ids) or workbench_inputs.get(node_id, [])
            dependencies = self._expand_workflow_dependencies(dependencies, workflow_node_ids)

            node = _V2PipelineNode(
                node_id=node_id,
                class_name=class_name,
                connection_name=str(raw_node.get("connection_name") or node_id),
                inputs=inputs,
                selected_action=str(
                    raw_node.get("selected_action")
                    or internal_data.get("selected_action")
                    or ""
                ).strip(),
                terminal=bool(raw_node.get("terminal", False) or node_id in terminal_source_ids),
                dependencies=list(dict.fromkeys(dependencies)),
                filter_expr=str(raw_node.get("filter") or ""),
                transform_expr=str(raw_node.get("transform") or ""),
                internal_data=dict(internal_data),
            )
            if class_name == "__ticker__":
                node.data_source_class = InlineTickerWorkflowSource
            else:
                node.data_source_class = import_datasource_class(class_name, project_root=os.getcwd())
            self.nodes[node_id] = node

    def _expand_workflow_dependencies(
        self,
        dependencies: List[str],
        workflow_node_ids: set[str],
        seen: Optional[set[str]] = None,
    ) -> List[str]:
        seen = set(seen or set())
        expanded: List[str] = []
        for dep_id in dependencies:
            dep_id = str(dep_id or "").strip()
            if not dep_id or dep_id in seen:
                continue
            if dep_id in self._workflow_start_node_ids or dep_id in self._workflow_end_node_ids:
                continue
            if dep_id in self._workflow_iterator_nodes:
                iterator_deps = self._expand_workflow_dependencies(
                    self._workflow_iterator_nodes[dep_id].get("dependencies", []),
                    workflow_node_ids,
                    seen | {dep_id},
                )
                for iterator_dep in iterator_deps:
                    if iterator_dep not in expanded:
                        expanded.append(iterator_dep)
                continue
            if dep_id in self._workflow_accumulator_nodes:
                accumulator_deps = self._expand_workflow_dependencies(
                    self._workflow_accumulator_nodes[dep_id].get("dependencies", []),
                    workflow_node_ids,
                    seen | {dep_id},
                )
                for accumulator_dep in accumulator_deps:
                    if accumulator_dep not in expanded:
                        expanded.append(accumulator_dep)
                continue
            if dep_id in workflow_node_ids:
                continue
            if dep_id not in expanded:
                expanded.append(dep_id)
        return expanded

    def _extract_dependencies(self, inputs: Dict[str, Any], known_node_ids: Optional[set[str]] = None) -> List[str]:
        deps = []
        if not isinstance(inputs, dict):
            return deps
        valid_node_ids = set(known_node_ids or set())

        def _collect(value: Any):
            if isinstance(value, dict) and "from" in value:
                _collect(value.get("from"))
                return
            if isinstance(value, list):
                for item in value:
                    _collect(item)
                return
            if isinstance(value, str) and "." in value and not value.startswith("$"):
                dep_id = value.split(".", 1)[0].strip()
                if dep_id and (not valid_node_ids or dep_id in valid_node_ids) and dep_id not in deps:
                    deps.append(dep_id)

        for value in inputs.values():
            _collect(value)
        return deps

    def _topological_order(self) -> List[str]:
        remaining = {node_id: set(node.dependencies) for node_id, node in self.nodes.items()}
        order = []
        while remaining:
            ready = sorted([node_id for node_id, deps in remaining.items() if not deps])
            if not ready:
                raise ValueError("Circular dependency detected in v2 pipeline")
            for node_id in ready:
                order.append(node_id)
                remaining.pop(node_id, None)
                for deps in remaining.values():
                    deps.discard(node_id)
        return order

    def _ensure_node_dirs(self, node_id: str):
        node_root = os.path.join(self.workspace_root, node_id)
        input_dir = os.path.join(node_root, "input")
        output_dir = os.path.join(node_root, "output")
        scratch_dir = os.path.join(node_root, "scratch")
        os.makedirs(input_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(scratch_dir, exist_ok=True)
        return input_dir, output_dir, scratch_dir

    def _resolve_connection_data(self, connection_name: str) -> Dict[str, Any]:
        record = self._connection_records.get(connection_name) or {}
        connection_data = record.get("connection_data")
        internal_data = record.get("internal_data")
        discovery_depth = None
        if isinstance(internal_data, dict):
            discovery_depth = internal_data.get("discovery_depth")
        if isinstance(connection_data, dict):
            resolved = dict(connection_data)
            if discovery_depth not in (None, "") and "discovery_depth" not in resolved:
                resolved["discovery_depth"] = discovery_depth
            return resolved
        if isinstance(internal_data, dict):
            nested = internal_data.get("connection_data")
            if isinstance(nested, dict):
                resolved = dict(nested)
                if discovery_depth not in (None, "") and "discovery_depth" not in resolved:
                    resolved["discovery_depth"] = discovery_depth
                return resolved
            return dict(internal_data)
        return {}

    def _instantiate_node(self, node: _V2PipelineNode):
        config = self._runtime_config(node)
        if getattr(node, "internal_data", None):
            config["internal_data"] = dict(node.internal_data)
        config["pipeline_stop_event"] = self._stop_event
        config["pipeline_started_check"] = lambda: self._started
        config["pipeline_name"] = self.pipeline_name
        connection_data = self._resolve_connection_data(node.connection_name)
        api_version = getattr(node.data_source_class, "api_version", None)
        is_v2 = callable(api_version) and node.data_source_class.api_version() == "v2"
        BBLogger.log(
            f"[Pipeline:instantiate] {node.node_id}: detected API={'v2' if is_v2 else 'v1'} "
            f"for {getattr(node.data_source_class, '__name__', node.class_name)}"
        )

        try:
            if is_v2:
                return node.data_source_class(connection=connection_data, config=config)

            params = dict(connection_data)
            params["connection_name"] = node.connection_name
            params["TARGET_DIRECTORY"] = config["output_dir"]
            params["target_directory"] = config["output_dir"]
            params["context_dir"] = config["output_dir"]
            params["ds_connection_tmp_space"] = self.workspace_root
            kwargs = {
                "name": node.node_id,
                "params": params,
            }
            try:
                signature = inspect.signature(node.data_source_class.__init__)
                accepts_var_kw = any(
                    parameter.kind == parameter.VAR_KEYWORD
                    for parameter in signature.parameters.values()
                )
                if not accepts_var_kw:
                    kwargs = {
                        key: value
                        for key, value in kwargs.items()
                        if key in signature.parameters
                    }
            except Exception:
                pass
            return node.data_source_class(**kwargs) if kwargs else node.data_source_class()
        except TypeError as exc:
            BBLogger.log(
                f"[Pipeline:instantiate] {node.node_id}: constructor signature mismatch: {exc}\n"
                f"{traceback.format_exc()}"
            )
            raise
        except Exception as exc:
            BBLogger.log(
                f"[Pipeline:instantiate] {node.node_id}: init failed: {exc}\n"
                f"{traceback.format_exc()}"
            )
            raise

    def _runtime_config(self, node: _V2PipelineNode) -> Dict[str, str]:
        input_dir, output_dir, scratch_dir = self._ensure_node_dirs(node.node_id)
        return {
            "input_dir": input_dir,
            "output_dir": output_dir,
            "scratch_dir": scratch_dir,
            "connection_name": node.connection_name,
            "context_dir": self.context_dir,
            "TARGET_DIRECTORY": self.context_dir,
        }

    def _build_request_for_node(
        self,
        node: _V2PipelineNode,
        results: Dict[str, Any],
        initial_request: Dict[str, Any],
    ) -> tuple[Dict[str, Any] | object, List[Dict[str, Any]]]:
        iteration_specs: List[Dict[str, Any]] = []
        if node.inputs:
            request: Dict[str, Any] = {}
            for key, raw_value in node.inputs.items():
                resolved = self._resolve_input_value(node, raw_value, results)
                if resolved is _SKIP_PIPELINE_NODE:
                    return _SKIP_PIPELINE_NODE, []
                if resolved is _SKIP_PIPELINE_INPUT:
                    continue
                if key in ("*", "$default") and isinstance(resolved, dict):
                    request.update(resolved)
                elif key in ("*", "$default"):
                    request["value"] = resolved
                else:
                    request[key] = resolved
                    iteration_spec = self._build_iteration_spec(key, raw_value, resolved)
                    if iteration_spec is not None:
                        iteration_specs.append(iteration_spec)
        elif node.dependencies:
            request = {}
            for dep_id in node.dependencies:
                dep_value = results.get(dep_id)
                if isinstance(dep_value, dict):
                    request.update(dep_value)
                elif dep_value is not None:
                    request[dep_id] = dep_value
        else:
            request = dict(initial_request)
        return request, iteration_specs

    def _build_iteration_spec(self, request_key: str, raw_value: Any, resolved: Any) -> Dict[str, Any] | None:
        if request_key in ("*", "$default"):
            return None
        if not isinstance(resolved, list):
            return None

        source_ref = ""
        if isinstance(raw_value, dict) and "from" in raw_value:
            source_ref = str(raw_value.get("from") or "").strip()
        elif isinstance(raw_value, str):
            source_ref = str(raw_value).strip()

        if not source_ref or "." not in source_ref or source_ref.startswith("$"):
            return None

        source_node_id, source_port = source_ref.split(".", 1)
        source_node_id = source_node_id.strip()
        source_port = source_port.strip()
        if source_node_id not in self.nodes and source_node_id not in self._workflow_iterator_nodes:
            return None

        if "[]" in source_port:
            array_root = source_port.split("[]", 1)[0].rstrip(".")
            if not array_root:
                return None

            return {
                "request_key": str(request_key),
                "values": list(resolved),
                "source_node_id": source_node_id,
                "source_port": source_port,
                "array_root": array_root,
                "safe_array_root": self._sanitize_iteration_root(array_root),
            }

        if source_node_id in self._iterated_nodes or source_node_id in self._workflow_iterator_nodes:
            return {
                "request_key": str(request_key),
                "values": list(resolved),
                "source_node_id": source_node_id,
                "source_port": source_port,
                "array_root": source_port,
                "safe_array_root": self._sanitize_iteration_root(source_port),
            }

        return None

    def _sanitize_iteration_root(self, root_name: str) -> str:
        return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(root_name or "")).strip("_.-") or "items"

    def _expand_iteration_requests(
        self,
        node: _V2PipelineNode,
        request: Dict[str, Any],
        iteration_specs: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        base_request = copy.deepcopy(dict(request or {}))
        if not iteration_specs:
            return [{"request": base_request, "input_dir": None}]

        iteration_count = max((len(spec.get("values", [])) for spec in iteration_specs), default=0)
        if iteration_count <= 0:
            return []

        plans: List[Dict[str, Any]] = []
        for iteration_index in range(iteration_count):
            iteration_request = copy.deepcopy(base_request)
            for spec in iteration_specs:
                request_key = str(spec.get("request_key") or "").strip()
                values = spec.get("values", [])
                if not request_key:
                    continue
                if iteration_index >= len(values):
                    iteration_request.pop(request_key, None)
                    continue
                iteration_request[request_key] = copy.deepcopy(values[iteration_index])
            plans.append(
                {
                    "request": iteration_request,
                    "input_dir": self._prepare_iteration_input_dir(node, iteration_specs, iteration_index),
                }
            )
        return plans

    def _prepare_iteration_input_dir(
        self,
        node: _V2PipelineNode,
        iteration_specs: List[Dict[str, Any]],
        iteration_index: int,
    ) -> str:
        base_input_dir, _, scratch_dir = self._ensure_node_dirs(node.node_id)
        iteration_root = os.path.join(scratch_dir, "__iter_inputs__")
        os.makedirs(iteration_root, exist_ok=True)
        iteration_input_dir = os.path.join(iteration_root, f"{node.node_id}_{iteration_index:04d}")
        if os.path.isdir(iteration_input_dir):
            shutil.rmtree(iteration_input_dir)
        os.makedirs(iteration_input_dir, exist_ok=True)

        if not os.path.isdir(base_input_dir):
            return iteration_input_dir

        array_specs = {
            (str(spec.get("source_node_id") or "").strip(), str(spec.get("safe_array_root") or "").strip())
            for spec in iteration_specs
            if str(spec.get("source_node_id") or "").strip() and str(spec.get("safe_array_root") or "").strip()
        }

        for file_name in os.listdir(base_input_dir):
            source_path = os.path.join(base_input_dir, file_name)
            if not os.path.isfile(source_path):
                continue

            matched_any = False
            matched_index = False
            for source_node_id, safe_array_root in array_specs:
                file_iteration_index = self._match_iteration_file_index(file_name, source_node_id, safe_array_root)
                if file_iteration_index is None:
                    continue
                matched_any = True
                if file_iteration_index == iteration_index:
                    matched_index = True

            if matched_any and not matched_index:
                continue

            shutil.copy2(source_path, os.path.join(iteration_input_dir, file_name))

        return iteration_input_dir

    def _match_iteration_file_index(self, file_name: str, source_node_id: str, safe_array_root: str) -> Optional[int]:
        marker = f"-{source_node_id}."
        if marker not in file_name:
            return None
        original_name = file_name.split(marker, 1)[1]
        match = re.match(
            rf"^{re.escape(safe_array_root)}_(\d+)(?:\..*)?$",
            original_name,
        )
        if not match:
            return None
        try:
            return int(match.group(1))
        except Exception:
            return None

    def _prepare_request_for_execution(
        self,
        node: _V2PipelineNode,
        request: Dict[str, Any],
        input_dir_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        prepared_request = dict(request or {})
        selected_action = str(getattr(node, "selected_action", "") or "").strip()
        if selected_action and selected_action not in {"All Ports", "__all_ports__"}:
            prepared_request["_action"] = selected_action

        if node.filter_expr:
            try:
                allowed = eval(
                    node.filter_expr,
                    {"__builtins__": {}},
                    {"data": prepared_request, "str": str, "int": int, "float": float},
                )
                if not allowed:
                    return {}
            except Exception as exc:
                BBLogger.log(f"Pipeline filter error for '{node.node_id}': {exc}")

        if node.transform_expr:
            try:
                transformed = eval(
                    node.transform_expr,
                    {"__builtins__": {}},
                    {"data": prepared_request, "str": str, "int": int, "float": float},
                )
                if isinstance(transformed, dict):
                    prepared_request = transformed
            except Exception as exc:
                BBLogger.log(f"Pipeline transform error for '{node.node_id}': {exc}")

        return self._stage_request_files(node, prepared_request, input_dir_override=input_dir_override)

    def _aggregate_iteration_results(self, results: List[Any]) -> Any:
        if not results:
            return []
        if all(isinstance(result, dict) for result in results):
            aggregated: Dict[str, List[Any]] = {}
            ordered_keys: List[str] = []
            for result in results:
                for key in result.keys():
                    if key not in ordered_keys:
                        ordered_keys.append(key)
            for key in ordered_keys:
                aggregated[key] = [result.get(key) if isinstance(result, dict) else None for result in results]
            return aggregated
        return list(results)

    def _resolve_input_value(self, node: _V2PipelineNode, raw_value: Any, results: Dict[str, Any]):
        if isinstance(raw_value, dict) and "from" in raw_value:
            ref_value = raw_value.get("from")
            expr = str(raw_value.get("expr") or "").strip()
            source_data = self._source_result_for_reference(ref_value, results)
            resolved = self._resolve_input_value(node, ref_value, results)
            if resolved is _SKIP_PIPELINE_NODE or resolved is _SKIP_PIPELINE_INPUT:
                return resolved
            if expr:
                evaluated = self._eval_edge_expr(expr, resolved, source_data)
                if evaluated is False:
                    return _SKIP_PIPELINE_NODE
                if evaluated is None:
                    return _SKIP_PIPELINE_INPUT
                return evaluated
            return resolved
        if not isinstance(raw_value, str):
            return raw_value
        if raw_value.startswith("$env."):
            return os.environ.get(raw_value[5:], "")
        if "." not in raw_value or raw_value.startswith("$"):
            return raw_value
        source_node_id, field_name = raw_value.split(".", 1)
        source_node_id = source_node_id.strip()
        field_name = field_name.strip()
        if source_node_id in self._workflow_start_node_ids:
            return _SKIP_PIPELINE_INPUT
        if source_node_id in self._workflow_accumulator_nodes:
            return self._resolve_accumulator_output(source_node_id, field_name, results)
        if source_node_id in self._workflow_iterator_nodes:
            return self._resolve_iterator_output(source_node_id, field_name, results)
        if source_node_id not in self.nodes:
            return raw_value
        source_value = results.get(source_node_id)
        if field_name == "*":
            return self._remap_result_files(node, source_node_id, source_value)
        if isinstance(source_value, dict):
            return self._remap_result_files(node, source_node_id, source_value.get(field_name))
        return None

    def _source_result_for_reference(self, raw_value: Any, results: Dict[str, Any]) -> Any:
        if not isinstance(raw_value, str):
            return None
        if "." not in raw_value or raw_value.startswith("$"):
            return None
        source_node_id, field_name = raw_value.split(".", 1)
        source_node_id = source_node_id.strip()
        field_name = field_name.strip()
        if source_node_id in self._workflow_accumulator_nodes:
            ready_value = results.get(source_node_id)
            if isinstance(ready_value, dict):
                return ready_value
            return {"output": ready_value} if ready_value is not None else None
        if source_node_id in self._workflow_iterator_nodes:
            return {field_name: self._resolve_iterator_output(source_node_id, field_name, results)}
        if source_node_id not in self.nodes:
            return None
        return results.get(source_node_id)

    def _resolve_accumulator_output(
        self,
        accumulator_node_id: str,
        field_name: str,
        results: Dict[str, Any],
        skip_if_unready: bool = True,
    ):
        accumulator_result = results.get(accumulator_node_id)
        if accumulator_result is None:
            return _SKIP_PIPELINE_NODE if skip_if_unready else None

        if isinstance(accumulator_result, dict):
            if field_name in {"output", "*"}:
                return copy.deepcopy(accumulator_result.get("output"))
            return copy.deepcopy(accumulator_result.get(field_name))
        if field_name in {"output", "*"}:
            return copy.deepcopy(accumulator_result)
        return None

    def _resolve_iterator_output(self, iterator_node_id: str, field_name: str, results: Dict[str, Any]):
        iterator_node = self._workflow_iterator_nodes.get(iterator_node_id) or {}
        raw_inputs = iterator_node.get("inputs", {})
        if not isinstance(raw_inputs, dict):
            raw_inputs = {}
        if field_name not in {"current_item", "*"}:
            return None

        items: List[Any] = []
        blocked = False
        for port_name in self._sorted_iterator_port_names(raw_inputs):
            raw_port_value = raw_inputs.get(port_name)
            for candidate in self._iter_iterator_raw_values(raw_port_value):
                resolved = self._resolve_iterator_input_reference(candidate, results)
                if resolved in (_SKIP_PIPELINE_INPUT, None):
                    continue
                if resolved is _SKIP_PIPELINE_NODE:
                    blocked = True
                    continue
                if isinstance(resolved, list):
                    items.extend(copy.deepcopy(resolved))
                else:
                    items.append(copy.deepcopy(resolved))
        if blocked and not items:
            return _SKIP_PIPELINE_NODE
        return items

    def _sorted_iterator_port_names(self, raw_inputs: Dict[str, Any]) -> List[str]:
        def _key(port_name: str) -> tuple[int, int, str]:
            text = str(port_name or "").strip()
            if text == "items":
                return (0, 0, text)
            match = re.fullmatch(r"items_(\d+)", text)
            if match:
                return (0, int(match.group(1)), text)
            return (1, 0, text)

        return sorted((str(name or "").strip() for name in raw_inputs.keys()), key=_key)

    def _iter_iterator_raw_values(self, raw_value: Any) -> List[Any]:
        if isinstance(raw_value, list):
            return list(raw_value)
        return [raw_value]

    def _resolve_iterator_input_reference(self, raw_value: Any, results: Dict[str, Any]):
        if isinstance(raw_value, dict) and "from" in raw_value:
            raw_value = raw_value.get("from")
        if not isinstance(raw_value, str):
            return raw_value
        if "." not in raw_value or raw_value.startswith("$"):
            return raw_value
        source_node_id, field_name = raw_value.split(".", 1)
        source_node_id = source_node_id.strip()
        field_name = field_name.strip()
        if source_node_id in self._workflow_accumulator_nodes:
            return self._resolve_accumulator_output(source_node_id, field_name, results)
        source_value = results.get(source_node_id)
        if isinstance(source_value, dict):
            return source_value.get(field_name)
        if field_name == "*":
            return source_value
        return None

    def _eval_edge_expr(self, expr: str, value: Any, source_data: Any) -> Any:
        import os.path as _os_path

        class _SafeOS:
            path = _os_path

        local_data = source_data if isinstance(source_data, dict) else {"value": value}
        try:
            return eval(
                expr,
                {"__builtins__": {}},
                {
                    "value": value,
                    "data": local_data,
                    "str": str,
                    "int": int,
                    "float": float,
                    "os": _SafeOS,
                },
            )
        except Exception as exc:
            BBLogger.log(f"Error evaluating edge expression '{expr}': {exc}")
            return value

    def _remap_result_files(self, node: _V2PipelineNode, source_node_id: str, value: Any):
        if isinstance(value, dict):
            return {
                key: self._remap_result_files(node, source_node_id, child)
                for key, child in value.items()
            }
        if isinstance(value, list):
            return [self._remap_result_files(node, source_node_id, child) for child in value]
        if isinstance(value, str):
            return self._resolve_routed_input_file(node, source_node_id, value)
        return value

    def _resolve_routed_input_file(
        self,
        node: _V2PipelineNode,
        source_node_id: str,
        value: str,
        input_dir_override: Optional[str] = None,
    ) -> str:
        if not value:
            return value
        if os.path.isfile(value):
            return value

        input_dir = input_dir_override or self._ensure_node_dirs(node.node_id)[0]
        basename = os.path.basename(value)
        if not basename or not os.path.isdir(input_dir):
            return value

        preferred_suffix = f"-{source_node_id}.{basename}"
        candidates = []
        for name in os.listdir(input_dir):
            full_path = os.path.join(input_dir, name)
            if not os.path.isfile(full_path):
                continue
            if name.endswith(preferred_suffix):
                candidates.append(full_path)
            elif name.endswith(f".{basename}"):
                candidates.append(full_path)

        if not candidates:
            return value
        return sorted(candidates)[-1]

    def _stage_request_files(
        self,
        node: _V2PipelineNode,
        request: Dict[str, Any],
        input_dir_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        input_dir = input_dir_override or self._ensure_node_dirs(node.node_id)[0]

        def _convert(value):
            if isinstance(value, dict):
                return {k: _convert(v) for k, v in value.items()}
            if isinstance(value, list):
                return [_convert(v) for v in value]
            if isinstance(value, str) and value and os.path.isfile(value):
                if os.path.abspath(os.path.dirname(value)) == os.path.abspath(input_dir):
                    return value
                destination = self._unique_destination(input_dir, os.path.basename(value))
                shutil.copy2(value, destination)
                return destination
            if isinstance(value, str):
                basename = os.path.basename(value)
                if basename:
                    remapped = self._resolve_routed_input_file(
                        node,
                        "",
                        value,
                        input_dir_override=input_dir,
                    )
                    if remapped != value:
                        return remapped
            return value

        return _convert(request)

    def _execute_node(
        self,
        node: _V2PipelineNode,
        request: Dict[str, Any],
        input_dir_override: Optional[str] = None,
    ):
        if node.instance is None:
            node.instance = self._instantiate_node(node)

        api_version = getattr(node.data_source_class, "api_version", None)
        is_v2 = callable(api_version) and node.data_source_class.api_version() == "v2"

        original_input_dir = None
        config = getattr(node.instance, "_config", None)
        if input_dir_override:
            if not isinstance(config, dict):
                config = {}
                setattr(node.instance, "_config", config)
            original_input_dir = config.get("input_dir")
            config["input_dir"] = input_dir_override

        try:
            BBLogger.log(
                f"[Pipeline:execute] Running {node.node_id} "
                f"(api={'v2' if is_v2 else 'v1'}) with keys={sorted((request or {}).keys())} "
                f"input_dir_override={input_dir_override or ''}"
            )
            if is_v2:
                if node.instance.supports_streaming():
                    stream_results: List[Any] = []
                    for item in node.instance.stream(request):
                        stream_results.append(item)
                        self._write_result_envelope(node, item)
                        self._route_output_files(node)
                    if len(stream_results) > 1:
                        result = self._aggregate_iteration_results(stream_results)
                    else:
                        result = stream_results[0] if stream_results else None
                else:
                    result = node.instance.run(request)
            else:
                collector: List[Any] = []

                class _Collector:
                    def notify(self, data):
                        collector.append(data)

                    def update(self, data):
                        collector.append(data)

                try:
                    node.instance.subscribe(_Collector())
                except Exception:
                    pass

                params = getattr(node.instance, "params", {})
                if isinstance(params, dict):
                    params.update(request)
                    params["TARGET_DIRECTORY"] = self._runtime_config(node)["output_dir"]
                    params["context_dir"] = self._runtime_config(node)["output_dir"]
                    if input_dir_override:
                        params["input_dir"] = input_dir_override

                if request and hasattr(node.instance, "process_input"):
                    result = node.instance.process_input(request)
                    if result is None and collector:
                        result = collector[-1]
                else:
                    result = node.instance.fetch()
                    if result is None and collector:
                        result = collector[-1]
            if isinstance(result, dict):
                BBLogger.log(
                    f"[Pipeline:execute] {node.node_id} completed with result keys={sorted(result.keys())}"
                )
            else:
                BBLogger.log(
                    f"[Pipeline:execute] {node.node_id} completed with result type={type(result).__name__}"
                )
        except Exception as exc:
            BBLogger.log(
                f"[Pipeline:execute] {node.node_id} failed: {exc}\n{traceback.format_exc()}"
            )
            raise
        finally:
            if input_dir_override and isinstance(getattr(node.instance, "_config", None), dict):
                if original_input_dir in (None, ""):
                    node.instance._config.pop("input_dir", None)
                else:
                    node.instance._config["input_dir"] = original_input_dir

        if result is not None:
            self._write_result_envelope(node, result)
        return result

    def _write_result_envelope(self, node: _V2PipelineNode, result: Any):
        _, output_dir, _ = self._ensure_node_dirs(node.node_id)
        with open(os.path.join(output_dir, "output.json"), "w", encoding="utf-8") as fh:
            json.dump(result, fh, indent=2, default=str)

    def _get_downstream_nodes(self, source_node_id: str) -> List[_V2PipelineNode]:
        return [node for node in self.nodes.values() if source_node_id in node.dependencies]

    def _unique_destination(self, directory: str, file_name: str) -> str:
        base_name, extension = os.path.splitext(file_name)
        candidate = os.path.join(directory, file_name)
        counter = 1
        while os.path.exists(candidate):
            candidate = os.path.join(directory, f"{base_name}_{counter}{extension}")
            counter += 1
        return candidate

    def _route_output_files(self, node: _V2PipelineNode):
        _, output_dir, _ = self._ensure_node_dirs(node.node_id)
        if not os.path.isdir(output_dir):
            return
        files = [name for name in os.listdir(output_dir) if os.path.isfile(os.path.join(output_dir, name))]
        if not files:
            return

        downstream_nodes = self._get_downstream_nodes(node.node_id)
        if node.terminal or not downstream_nodes:
            os.makedirs(self.context_dir, exist_ok=True)
            for file_name in files:
                source_path = os.path.join(output_dir, file_name)
                destination = self._unique_destination(self.context_dir, f"{node.node_id}-{file_name}")
                shutil.move(source_path, destination)
            return

        stamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")[:23]
        for file_name in files:
            source_path = os.path.join(output_dir, file_name)
            destinations = []
            for downstream_node in downstream_nodes:
                input_dir, _, _ = self._ensure_node_dirs(downstream_node.node_id)
                destinations.append(
                    self._unique_destination(
                        input_dir,
                        f"{stamp}-{node.node_id}.{file_name}",
                    )
                )

            if len(destinations) == 1:
                shutil.move(source_path, destinations[0])
                continue

            for destination in destinations[:-1]:
                shutil.copy2(source_path, destination)
            shutil.move(source_path, destinations[-1])


class SubjectivePipelineDataSource(SubjectiveDataSource):
    @classmethod
    def connection_schema(cls) -> dict:
        return {
            "pipeline_name": {
                "type": "text",
                "label": "Pipeline Name",
                "required": False,
            },
            "pipeline_file": {
                "type": "file_path",
                "label": "Pipeline File",
                "required": False,
            },
            "pipeline_json_path": {
                "type": "file_path",
                "label": "Pipeline JSON File Path",
                "required": False,
            },
            "pipeline_json": {
                "type": "textarea",
                "label": "Pipeline JSON",
                "required": False,
            },
        }

    @classmethod
    def output_schema(cls) -> dict:
        return {
            "pipeline_result": {
                "type": "dict",
                "description": "Result returned by the pipeline terminal node(s)",
            }
        }

    @classmethod
    def icon(cls) -> str:
        return '''<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <rect x="3" y="4" width="6" height="6" rx="1" fill="#4F46E5"/>
            <rect x="15" y="4" width="6" height="6" rx="1" fill="#4F46E5"/>
            <rect x="9" y="14" width="6" height="6" rx="1" fill="#4F46E5"/>
            <path d="M6 10 L12 14 M18 10 L12 14" stroke="#4F46E5" stroke-width="2"/>
        </svg>'''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline = None
        self.pipeline_config = None
        self.pipeline_name = self._get_pipeline_settings().get("pipeline_name", "Pipeline")

    def fetch(self):
        return self.run({})

    def run(self, request: dict) -> Any:
        self._load_pipeline_config()
        if str(self.pipeline_config.get("version") or "").strip() == "2":
            try:
                BBLogger.log(f"[Pipeline] Creating V2 runner for pipeline '{self.pipeline_name}'")
                runner = _SubjectiveDataSourcePipelineRunner(
                    pipeline_name=self.pipeline_config.get("name", self.pipeline_name),
                    pipeline_config=self.pipeline_config,
                    context_dir=self.output_dir or self._resolve_context_path(),
                    tmp_root=self.get_connection_temp_dir(),
                )
                runner._crash_log(f"Runner created. nodes={len(runner.nodes)}")
                BBLogger.log(
                    f"[Pipeline] Runner created. nodes={len(runner.nodes)}, "
                    f"start={len(runner._workflow_start_node_ids)}, "
                    f"end={len(runner._workflow_end_node_ids)}, "
                    f"iterator={len(runner._workflow_iterator_nodes)}"
                )
                self.pipeline = runner.build()
                runner._crash_log("build() complete. Starting execution...")
                BBLogger.log("[Pipeline] build() complete. Starting execution...")
                result = self.pipeline.start(request or {})
                runner._crash_log("Execution finished.")
                BBLogger.log("[Pipeline] Execution finished.")
                return {"pipeline_result": result}
            except Exception as exc:
                msg = f"[Pipeline] FATAL: Pipeline execution failed: {exc}\n{traceback.format_exc()}"
                print(msg, file=_sys.stderr, flush=True)
                BBLogger.log(
                    f"[Pipeline] FATAL: Pipeline execution failed: {exc}\n{traceback.format_exc()}"
                )
                raise

        self._build_legacy_pipeline_from_config()
        self.pipeline.start()
        try:
            while getattr(self.pipeline, "_started", False):
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
        return {"pipeline_result": {"status": "running", "pipeline_name": self.pipeline_name}}

    def stop(self):
        if self.pipeline and hasattr(self.pipeline, "stop"):
            self.pipeline.stop()

    def get_data_source_type_name(self):
        return "Pipeline"

    def _get_pipeline_settings(self) -> Dict[str, Any]:
        settings = {}
        for source in (self._connection, self.params, self._config):
            if isinstance(source, dict):
                settings.update(source)
        return settings

    def _load_pipeline_config(self):
        settings = self._get_pipeline_settings()
        pipeline_path = settings.get("pipeline_json_path") or settings.get("pipeline_file")
        if pipeline_path:
            if not os.path.isfile(pipeline_path):
                raise FileNotFoundError(f"Pipeline file not found: {pipeline_path}")
            with open(pipeline_path, "r", encoding="utf-8") as fh:
                self.pipeline_config = json.load(fh)
            self.pipeline_name = self.pipeline_config.get("name", self.pipeline_name)
            return

        pipeline_json = settings.get("pipeline_json") or settings.get("pipeline_data")
        if pipeline_json:
            if isinstance(pipeline_json, str):
                self.pipeline_config = json.loads(pipeline_json)
            elif isinstance(pipeline_json, dict):
                self.pipeline_config = pipeline_json
            else:
                raise ValueError("pipeline_json must be a string or dict")
            self.pipeline_name = self.pipeline_config.get("name", self.pipeline_name)
            return

        raise ValueError("No pipeline configuration provided (pipeline_json, pipeline_json_path, or pipeline_file)")

    def _build_legacy_pipeline_from_config(self):
        if not self.pipeline_config or "nodes" not in self.pipeline_config:
            raise ValueError("Legacy pipeline config must include 'nodes'")
        self.pipeline_name = self.pipeline_config.get("name", self.pipeline_name)
        self.pipeline = SubjectiveDataSourcePipeline(name=self.pipeline_name)
        for node_config in self.pipeline_config.get("nodes", []):
            node_id = node_config.get("node_id")
            class_name = node_config.get("class")
            if not node_id or not class_name:
                raise ValueError("Legacy pipeline node must include 'node_id' and 'class'")
            data_source_class = import_datasource_class(class_name, project_root=os.getcwd())
            filter_fn = None
            transform_fn = None
            if node_config.get("filter"):
                filter_fn = self._create_filter_function(str(node_config["filter"]))
            if node_config.get("transform"):
                transform_fn = self._create_transform_function(str(node_config["transform"]))
            self.pipeline.add_node(
                node_id=node_id,
                data_source_class=data_source_class,
                params=node_config.get("params", {}) or {},
                dependencies=node_config.get("dependencies", []) or [],
                transform_fn=transform_fn,
                filter_fn=filter_fn,
            )
        self.pipeline.build()

    def _create_filter_function(self, filter_expr: str):
        def filter_fn(data):
            try:
                return eval(
                    filter_expr,
                    {"__builtins__": {}},
                    {"data": data, "str": str, "int": int, "float": float},
                )
            except Exception:
                return False

        return filter_fn

    def _create_transform_function(self, transform_expr: str):
        def transform_fn(data):
            try:
                return eval(
                    transform_expr,
                    {"__builtins__": {}},
                    {"data": data, "str": str, "int": int, "float": float},
                )
            except Exception:
                return data

        return transform_fn
