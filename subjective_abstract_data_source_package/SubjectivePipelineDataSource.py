"""
SubjectivePipelineDataSource

Legacy programmatic pipeline runner plus the v2 pipeline wrapper used by the
desktop app. The direct runner remains available for backward compatibility.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import importlib
import inspect
import json
import os
import shutil
import tempfile
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

from brainboost_data_source_logger_package.BBLogger import BBLogger

from subjective_abstract_data_source_package import SubjectiveDataSource
from subjective_abstract_data_source_package.datasource_importer import import_datasource_class


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
    terminal: bool = False
    dependencies: List[str] = field(default_factory=list)
    filter_expr: str = ""
    transform_expr: str = ""
    data_source_class: Any = None
    instance: Any = None


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
        self._build_nodes()

    def build(self):
        os.makedirs(self.workspace_root, exist_ok=True)
        for node in self.nodes.values():
            self._ensure_node_dirs(node.node_id)
            if node.instance is None:
                node.instance = self._instantiate_node(node)
        self._started = True
        return self

    def start(self, request: Optional[Dict[str, Any]] = None):
        return self.run(request or {})

    def run(self, request: Optional[Dict[str, Any]] = None):
        if not self._started:
            self.build()

        initial_request = dict(request or {})
        results: Dict[str, Any] = {}

        for node_id in self._topological_order():
            node = self.nodes[node_id]
            node_request = self._build_request_for_node(node, results, initial_request)
            result = self._execute_node(node, node_request)
            results[node_id] = result
            self._route_output_files(node)

        terminal_nodes = [n.node_id for n in self.nodes.values() if n.terminal]
        if not terminal_nodes and self.nodes:
            terminal_nodes = [self._topological_order()[-1]]
        if len(terminal_nodes) == 1:
            return results.get(terminal_nodes[0])
        return {node_id: results.get(node_id) for node_id in terminal_nodes}

    def stop(self):
        for node in self.nodes.values():
            if node.instance and hasattr(node.instance, "stop"):
                try:
                    node.instance.stop()
                except Exception as exc:
                    BBLogger.log(f"Error stopping pipeline node '{node.node_id}': {exc}")

    def get_node_instance(self, node_id: str):
        node = self.nodes.get(node_id)
        return node.instance if node else None

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

        workbench_inputs = {}
        workbench = self.pipeline_config.get("workbench", {}) or {}
        for conn in workbench.get("connections", []):
            if not isinstance(conn, dict):
                continue
            workbench_inputs.setdefault(str(conn.get("to_node") or ""), []).append(
                str(conn.get("from_node") or "")
            )

        for raw_node in nodes:
            if not isinstance(raw_node, dict):
                continue
            node_id = str(raw_node.get("node_id") or "").strip()
            class_name = str(raw_node.get("class") or "").strip()
            if not node_id or not class_name:
                raise ValueError("Each v2 pipeline node must define 'node_id' and 'class'")

            inputs = raw_node.get("inputs", {}) or {}
            if not isinstance(inputs, dict):
                inputs = {}
            dependencies = self._extract_dependencies(inputs) or workbench_inputs.get(node_id, [])

            node = _V2PipelineNode(
                node_id=node_id,
                class_name=class_name,
                connection_name=str(raw_node.get("connection_name") or node_id),
                inputs=inputs,
                terminal=bool(raw_node.get("terminal", False)),
                dependencies=list(dict.fromkeys(dependencies)),
                filter_expr=str(raw_node.get("filter") or ""),
                transform_expr=str(raw_node.get("transform") or ""),
            )
            node.data_source_class = import_datasource_class(class_name, project_root=os.getcwd())
            self.nodes[node_id] = node

    def _extract_dependencies(self, inputs: Dict[str, Any]) -> List[str]:
        deps = []
        for value in inputs.values():
            if isinstance(value, str) and "." in value and not value.startswith("$"):
                dep_id = value.split(".", 1)[0].strip()
                if dep_id and dep_id not in deps:
                    deps.append(dep_id)
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
        if isinstance(connection_data, dict):
            return dict(connection_data)
        internal_data = record.get("internal_data")
        if isinstance(internal_data, dict):
            nested = internal_data.get("connection_data")
            if isinstance(nested, dict):
                return dict(nested)
            return dict(internal_data)
        return {}

    def _instantiate_node(self, node: _V2PipelineNode):
        config = self._runtime_config(node)
        connection_data = self._resolve_connection_data(node.connection_name)
        api_version = getattr(node.data_source_class, "api_version", None)
        is_v2 = callable(api_version) and node.data_source_class.api_version() == "v2"

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
    ) -> Dict[str, Any]:
        if node.inputs:
            request: Dict[str, Any] = {}
            for key, raw_value in node.inputs.items():
                resolved = self._resolve_input_value(node, raw_value, results)
                if key in ("*", "$default") and isinstance(resolved, dict):
                    request.update(resolved)
                elif key in ("*", "$default"):
                    request["value"] = resolved
                else:
                    request[key] = resolved
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

        if node.filter_expr:
            try:
                allowed = eval(
                    node.filter_expr,
                    {"__builtins__": {}},
                    {"data": request, "str": str, "int": int, "float": float},
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
                    {"data": request, "str": str, "int": int, "float": float},
                )
                if isinstance(transformed, dict):
                    request = transformed
            except Exception as exc:
                BBLogger.log(f"Pipeline transform error for '{node.node_id}': {exc}")

        return self._stage_request_files(node, request)

    def _resolve_input_value(self, node: _V2PipelineNode, raw_value: Any, results: Dict[str, Any]):
        if not isinstance(raw_value, str):
            return raw_value
        if raw_value.startswith("$env."):
            return os.environ.get(raw_value[5:], "")
        if "." not in raw_value or raw_value.startswith("$"):
            return raw_value
        source_node_id, field_name = raw_value.split(".", 1)
        source_value = results.get(source_node_id)
        if field_name == "*":
            return self._remap_result_files(node, source_node_id, source_value)
        if isinstance(source_value, dict):
            return self._remap_result_files(node, source_node_id, source_value.get(field_name))
        return None

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

    def _resolve_routed_input_file(self, node: _V2PipelineNode, source_node_id: str, value: str) -> str:
        if not value:
            return value
        if os.path.isfile(value):
            return value

        input_dir, _, _ = self._ensure_node_dirs(node.node_id)
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

    def _stage_request_files(self, node: _V2PipelineNode, request: Dict[str, Any]) -> Dict[str, Any]:
        input_dir, _, _ = self._ensure_node_dirs(node.node_id)

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
                    remapped = self._resolve_routed_input_file(node, "", value)
                    if remapped != value:
                        return remapped
            return value

        return _convert(request)

    def _execute_node(self, node: _V2PipelineNode, request: Dict[str, Any]):
        if node.instance is None:
            node.instance = self._instantiate_node(node)

        api_version = getattr(node.data_source_class, "api_version", None)
        is_v2 = callable(api_version) and node.data_source_class.api_version() == "v2"

        if is_v2:
            if node.instance.supports_streaming():
                results = list(node.instance.stream(request))
                result = results[-1] if results else None
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

            if request and hasattr(node.instance, "process_input"):
                result = node.instance.process_input(request)
                if result is None and collector:
                    result = collector[-1]
            else:
                result = node.instance.fetch()
                if result is None and collector:
                    result = collector[-1]

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
            runner = _SubjectiveDataSourcePipelineRunner(
                pipeline_name=self.pipeline_config.get("name", self.pipeline_name),
                pipeline_config=self.pipeline_config,
                context_dir=self.output_dir or self._resolve_context_path(),
                tmp_root=self.get_connection_temp_dir(),
            )
            self.pipeline = runner.build()
            result = self.pipeline.start(request or {})
            return {"pipeline_result": result}

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

        pipeline_path = settings.get("pipeline_json_path") or settings.get("pipeline_file")
        if pipeline_path:
            with open(pipeline_path, "r", encoding="utf-8") as fh:
                self.pipeline_config = json.load(fh)
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
