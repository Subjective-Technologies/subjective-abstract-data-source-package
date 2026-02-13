"""
SubjectivePipelineDataSource

A data source that loads and executes pipelines defined in JSON format.
This allows pipelines to be managed from the UI like any other data source.
"""

import json
import os
from typing import Dict, Any, Optional, List, Callable
from subjective_abstract_data_source_package import SubjectiveDataSource
from subjective_abstract_data_source_package.datasource_importer import import_datasource_class
from brainboost_data_source_logger_package.BBLogger import BBLogger
import importlib
import threading


class PipelineNode:
    """
    Represents a node in the data pipeline with its data source instance,
    dependencies, and data transformation logic.
    """

    def __init__(
        self,
        node_id: str,
        data_source_class,
        params: Optional[Dict[str, Any]] = None,
        dependencies: Optional[List[str]] = None,
        transform_fn: Optional[Callable] = None,
        filter_fn: Optional[Callable] = None
    ):
        self.node_id = node_id
        self.data_source_class = data_source_class
        self.params = params or {}
        self.dependencies = dependencies or []
        self.transform_fn = transform_fn
        self.filter_fn = filter_fn
        self.instance = None

    def should_process(self, data: Any) -> bool:
        if self.filter_fn is None:
            return True
        try:
            return self.filter_fn(data)
        except Exception as e:
            BBLogger.log(f"Filter function error in node {self.node_id}: {e}")
            return False

    def transform_data(self, data: Any) -> Any:
        if self.transform_fn is None:
            return data
        try:
            return self.transform_fn(data)
        except Exception as e:
            BBLogger.log(f"Transform function error in node {self.node_id}: {e}")
            return data


class PipelineAdapter:
    """
    Adapter that connects a dependent data source to a dependency data source.
    Receives updates from the dependency and triggers the dependent data source.
    """

    def __init__(self, pipeline_node: PipelineNode, pipeline: 'SubjectiveDataSourcePipeline'):
        self.pipeline_node = pipeline_node
        self.pipeline = pipeline

    def update(self, data: Any):
        try:
            BBLogger.log(f"PipelineAdapter for {self.pipeline_node.node_id} received update: {data}")

            if not self.pipeline_node.should_process(data):
                BBLogger.log(f"Node {self.pipeline_node.node_id} filtered out update")
                return

            transformed_data = self.pipeline_node.transform_data(data)
            self.pipeline._trigger_node(self.pipeline_node.node_id, transformed_data)

        except Exception as e:
            BBLogger.log(f"Error in PipelineAdapter for {self.pipeline_node.node_id}: {e}")

    def notify(self, data: Any):
        self.update(data)


class SubjectiveDataSourcePipeline(SubjectiveDataSource):
    """
    Pipeline orchestrator that inherits from SubjectiveDataSource.
    """

    def __init__(
        self,
        name: str = None,
        session=None,
        dependency_data_sources: List[SubjectiveDataSource] = None,
        subscribers=None,
        params: Dict[str, Any] = None
    ):
        super().__init__(
            name=name,
            session=session,
            dependency_data_sources=dependency_data_sources or [],
            subscribers=subscribers,
            params=params or {}
        )

        self.nodes: Dict[str, PipelineNode] = {}
        self.adapters: Dict[str, List[PipelineAdapter]] = {}
        self._started = False
        self._pipeline_config = None
        self._pipeline_file_path = None

        pipeline_file = self.params.get('pipeline_file')
        if pipeline_file:
            self._pipeline_file_path = pipeline_file
            self._load_from_pipe_file(pipeline_file)

        BBLogger.log(f"Pipeline '{self.get_name()}' initialized")

    def _load_from_pipe_file(self, file_path: str):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                self._pipeline_config = json.load(f)

            BBLogger.log(f"Loaded pipeline configuration from {file_path}")

            self._build_from_config()

        except FileNotFoundError:
            raise ValueError(f"Pipeline file not found: {file_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in pipeline file: {e}")
        except Exception as e:
            raise ValueError(f"Error loading pipeline file: {e}")

    def _build_from_config(self):
        if not self._pipeline_config:
            raise ValueError("No pipeline configuration loaded")

        if 'nodes' not in self._pipeline_config:
            raise ValueError("Pipeline config must have 'nodes' key")

        if 'name' in self._pipeline_config and not self.name:
            self.name = self._pipeline_config['name']

        BBLogger.log(f"Building pipeline from config: {self.get_name()}")

        nodes = self._pipeline_config['nodes']
        for node_config in nodes:
            self._add_node_from_config(node_config)

        BBLogger.log(f"Pipeline built with {len(nodes)} nodes")

    def _add_node_from_config(self, node_config: Dict[str, Any]):
        node_id = node_config.get('node_id')
        if not node_id:
            raise ValueError("Each node must have a 'node_id'")

        module_path = node_config.get('module')
        class_name = node_config.get('class')

        if not module_path or not class_name:
            raise ValueError(f"Node '{node_id}' must specify 'module' and 'class'")

        try:
            module = importlib.import_module(module_path)
            data_source_class = getattr(module, class_name)
        except (ImportError, AttributeError) as e:
            BBLogger.log(f"Failed to load class {class_name} from {module_path}: {e}")
            raise ValueError(f"Could not load data source class: {e}")

        params = node_config.get('params', {})
        dependencies = node_config.get('dependencies', [])

        filter_fn = None
        filter_expr = node_config.get('filter')
        if filter_expr:
            filter_fn = self._create_filter_function(filter_expr)

        transform_fn = None
        transform_expr = node_config.get('transform')
        if transform_expr:
            transform_fn = self._create_transform_function(transform_expr)

        self.add_node(
            node_id=node_id,
            data_source_class=data_source_class,
            params=params,
            dependencies=dependencies,
            filter_fn=filter_fn,
            transform_fn=transform_fn
        )

        BBLogger.log(f"Added node '{node_id}' to pipeline")

    def _create_filter_function(self, filter_expr: str):
        def filter_fn(data):
            try:
                return eval(filter_expr, {"__builtins__": {}}, {"data": data, "str": str, "int": int, "float": float})
            except Exception as e:
                BBLogger.log(f"Error evaluating filter expression '{filter_expr}': {e}")
                return False

        return filter_fn

    def _create_transform_function(self, transform_expr: str):
        def transform_fn(data):
            try:
                return eval(transform_expr, {"__builtins__": {}}, {"data": data, "str": str, "int": int, "float": float})
            except Exception as e:
                BBLogger.log(f"Error evaluating transform expression '{transform_expr}': {e}")
                return data

        return transform_fn

    def add_node(
        self,
        node_id: str,
        data_source_class,
        params: Optional[Dict[str, Any]] = None,
        dependencies: Optional[List[str]] = None,
        transform_fn: Optional[Callable] = None,
        filter_fn: Optional[Callable] = None
    ) -> 'SubjectiveDataSourcePipeline':
        if node_id in self.nodes:
            raise ValueError(f"Node with id '{node_id}' already exists in pipeline")

        node = PipelineNode(
            node_id=node_id,
            data_source_class=data_source_class,
            params=params,
            dependencies=dependencies,
            transform_fn=transform_fn,
            filter_fn=filter_fn
        )

        self.nodes[node_id] = node
        BBLogger.log(f"Added node '{node_id}' to pipeline '{self.get_name()}'")

        return self

    def add_node_from_module(
        self,
        node_id: str,
        module_path: str,
        class_name: str,
        params: Optional[Dict[str, Any]] = None,
        dependencies: Optional[List[str]] = None,
        transform_fn: Optional[Callable] = None,
        filter_fn: Optional[Callable] = None
    ) -> 'SubjectiveDataSourcePipeline':
        try:
            module = importlib.import_module(module_path)
            data_source_class = getattr(module, class_name)

            return self.add_node(
                node_id=node_id,
                data_source_class=data_source_class,
                params=params,
                dependencies=dependencies,
                transform_fn=transform_fn,
                filter_fn=filter_fn
            )
        except (ImportError, AttributeError) as e:
            BBLogger.log(f"Failed to load data source class {class_name} from {module_path}: {e}")
            raise

    def _validate_dependencies(self):
        """Validate that all dependencies exist and there are no circular dependencies."""
        for node_id, node in self.nodes.items():
            for dep_id in node.dependencies:
                if dep_id not in self.nodes:
                    raise ValueError(f"Node '{node_id}' depends on non-existent node '{dep_id}'")

        # Check for circular dependencies using DFS
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
            if node_id not in visited:
                if has_cycle(node_id):
                    raise ValueError(f"Circular dependency detected involving node '{node_id}'")

    def build(self):
        """
        Build the pipeline by instantiating data sources and setting up dependencies.
        """
        if self._started:
            BBLogger.log(f"Pipeline '{self.get_name()}' already built")
            return

        BBLogger.log(f"Building pipeline '{self.get_name()}'")

        # Validate dependencies
        self._validate_dependencies()

        # Instantiate all data sources
        for node_id, node in self.nodes.items():
            try:
                BBLogger.log(f"Instantiating data source for node '{node_id}'")

                # Get dependency data source instances
                dependency_instances = [
                    self.nodes[dep_id].instance
                    for dep_id in node.dependencies
                ]

                # Create the data source instance, trimming args to supported signature.
                init_kwargs = {
                    "name": node_id,
                    "dependency_data_sources": dependency_instances,
                    "params": node.params,
                }
                try:
                    import inspect
                    signature = inspect.signature(node.data_source_class.__init__)
                    params = signature.parameters
                    accepts_var_kw = any(
                        p.kind == p.VAR_KEYWORD for p in params.values()
                    )
                    if not accepts_var_kw:
                        init_kwargs = {k: v for k, v in init_kwargs.items() if k in params}
                except Exception:
                    pass

                if init_kwargs:
                    node.instance = node.data_source_class(**init_kwargs)
                else:
                    node.instance = node.data_source_class()

                BBLogger.log(f"Instantiated {node.data_source_class.__name__} for node '{node_id}'")

            except Exception as e:
                BBLogger.log(f"Failed to instantiate data source for node '{node_id}': {e}")
                raise

        # Set up adapters for nodes with dependencies
        for node_id, node in self.nodes.items():
            if node.dependencies:
                for dep_id in node.dependencies:
                    # Create an adapter for this dependency relationship
                    adapter = PipelineAdapter(node, self)

                    # Subscribe the adapter to the dependency data source
                    dependency_node = self.nodes[dep_id]
                    dependency_node.instance.subscribe(adapter)

                    # Track the adapter
                    if node_id not in self.adapters:
                        self.adapters[node_id] = []
                    self.adapters[node_id].append(adapter)

                    BBLogger.log(f"Connected node '{node_id}' to dependency '{dep_id}'")

        self._started = True
        BBLogger.log(f"Pipeline '{self.get_name()}' built successfully")

        # Start Redis listeners if any nodes requested subscription
        try:
            self._start_redis_listeners()
        except Exception as e:
            BBLogger.log(f"Failed to start Redis listeners: {e}")

    def start(self):
        """
        Start the pipeline by starting all data sources.
        """
        if not self._started:
            self.build()

        BBLogger.log(f"Starting pipeline '{self.get_name()}'")

        # Start nodes in dependency order (dependencies first)
        started = set()

        def start_node(node_id):
            if node_id in started:
                return

            node = self.nodes[node_id]

            # Start dependencies first
            for dep_id in node.dependencies:
                start_node(dep_id)

            # Start this node
            try:
                BBLogger.log(f"Starting node '{node_id}'")

                # For real-time data sources, call fetch() which starts monitoring
                # For regular data sources, fetch() will process data
                if hasattr(node.instance, 'fetch'):
                    # Start in a separate thread for real-time sources to avoid blocking
                    thread = threading.Thread(
                        target=node.instance.fetch,
                        name=f"Pipeline-{self.get_name()}-{node_id}",
                        daemon=True
                    )
                    thread.start()

                    # Notify UI that node has started
                    try:
                        self._notify_node_started(node_id, pid=thread.ident)
                    except Exception:
                        pass

                started.add(node_id)
                BBLogger.log(f"Started node '{node_id}'")

            except Exception as e:
                BBLogger.log(f"Failed to start node '{node_id}': {e}")
                raise

        # Start all nodes
        for node_id in self.nodes:
            start_node(node_id)

        BBLogger.log(f"Pipeline '{self.get_name()}' started successfully")

    def stop(self):
        """
        Stop the pipeline by stopping all data sources.
        """
        BBLogger.log(f"Stopping pipeline '{self.get_name()}'")

        # Stop Redis listeners first
        try:
            self._stop_redis_listeners()
        except Exception as e:
            BBLogger.log(f"Error stopping Redis listeners: {e}")

        for node_id, node in self.nodes.items():
            try:
                if node.instance and hasattr(node.instance, 'stop'):
                    node.instance.stop()
                    BBLogger.log(f"Stopped node '{node_id}'")

                    # Notify UI that node has stopped
                    try:
                        self._notify_node_stopped(node_id)
                    except Exception:
                        pass

            except Exception as e:
                BBLogger.log(f"Error stopping node '{node_id}': {e}")

        self._started = False
        BBLogger.log(f"Pipeline '{self.get_name()}' stopped")

    def _trigger_node(self, node_id: str, data: Any):
        """
        Manually trigger a node with specific data.
        This is called by PipelineAdapters when dependency data arrives.

        Args:
            node_id: ID of the node to trigger
            data: Data to pass to the node
        """
        node = self.nodes.get(node_id)
        if not node or not node.instance:
            BBLogger.log(f"Cannot trigger node '{node_id}': not found or not instantiated")
            return

        try:
            BBLogger.log(f"Triggering node '{node_id}' with data: {data}")

            # If the data source has a process_input method, use it
            if hasattr(node.instance, 'process_input'):
                node.instance.process_input(data)
                BBLogger.log(f"process_input called on '{node_id}'")
            # Otherwise, call update to notify its subscribers
            elif hasattr(node.instance, 'update'):
                node.instance.update(data)
                BBLogger.log(f"update called on '{node_id}'")
            else:
                BBLogger.log(f"Node '{node_id}' has no process_input or update method")

        except Exception as e:
            BBLogger.log(f"Error triggering node '{node_id}': {e}")

    def get_node_instance(self, node_id: str):
        """
        Get the data source instance for a specific node.

        Args:
            node_id: ID of the node

        Returns:
            The data source instance or None if not found
        """
        node = self.nodes.get(node_id)
        return node.instance if node else None

    def fetch(self):
        """
        Fetch data by starting the pipeline. Implements SubjectiveDataSource.fetch
        """
        BBLogger.log(f"Fetch called on pipeline '{self.get_name()}'")
        self.start()
        BBLogger.log(f"Pipeline '{self.get_name()}' is running")
        import time
        try:
            while self._started:
                time.sleep(1)
        except KeyboardInterrupt:
            BBLogger.log("Pipeline interrupted by user")
            self.stop()

    def get_icon(self) -> str:
        """Return SVG icon for pipeline data source."""
        return '''<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <rect x="3" y="4" width="6" height="6" rx="1" fill="#4F46E5"/>
            <rect x="15" y="4" width="6" height="6" rx="1" fill="#4F46E5"/>
            <rect x="9" y="14" width="6" height="6" rx="1" fill="#4F46E5"/>
            <path d="M6 10 L12 14 M18 10 L12 14" stroke="#4F46E5" stroke-width="2"/>
        </svg>'''

    def get_connection_data(self) -> dict:
        """Return connection configuration for pipeline data source."""
        return {
            "connection_type": "PIPELINE",
            "fields": []
        }

    # === Redis subscription helpers ===

    def _start_redis_listeners(self):
        """
        Start Redis pubsub listeners for any nodes that request subscription.

        Nodes may specify 'redis_channel' in their params to request subscription
        to a particular channel. If present, the pipeline subscribes and forwards
        incoming messages to `handle_redis_payload` for processing.
        """
        try:
            import redis
        except ImportError:
            BBLogger.log("Redis library not available; skipping Redis listeners")
            return

        # Collect channels to subscribe
        channels = set()
        for node_id, node in self.nodes.items():
            channel = node.params.get('redis_channel') if node.params else None
            if channel:
                channels.add(channel)

        if not channels:
            BBLogger.log("No Redis channels configured for pipeline nodes")
            return

        BBLogger.log(f"Subscribing to Redis channels: {channels}")

        self._redis_stop_event = threading.Event()
        self._redis_threads = []

        def _listen(channel_name):
            try:
                client = redis.Redis()
                pubsub = client.pubsub()
                pubsub.subscribe(channel_name)
                BBLogger.log(f"Listening to Redis channel '{channel_name}'")

                while not self._redis_stop_event.is_set():
                    message = pubsub.get_message(timeout=1)
                    if message and message.get('type') == 'message':
                        try:
                            data = json.loads(message.get('data'))
                        except Exception:
                            BBLogger.log(f"Received non-JSON message on '{channel_name}': {message.get('data')}")
                            continue
                        self.handle_redis_payload(data)

            except Exception as e:
                BBLogger.log(f"Error in Redis listener for '{channel_name}': {e}")

        for ch in channels:
            t = threading.Thread(target=_listen, args=(ch,), daemon=True)
            t.start()
            self._redis_threads.append(t)

    def _stop_redis_listeners(self):
        """Stop any running Redis listener threads."""
        if hasattr(self, '_redis_stop_event'):
            self._redis_stop_event.set()
        if hasattr(self, '_redis_threads'):
            for t in self._redis_threads:
                t.join(timeout=2)
            BBLogger.log("Redis listener threads stopped")

    def handle_redis_payload(self, payload: dict):
        """
        Public method to process incoming Redis payloads (dict).
        This preserves the payload and routes it to downstream nodes.
        """
        try:
            BBLogger.log(f"Redis event received: {payload}")

            # Resolve emitting node by matching connection_name or datasource_name
            emitting_node_id = None
            for node_id, node in self.nodes.items():
                node_conn_name = node.params.get('connection_name') if node.params else None
                node_ds_name = node.params.get('datasource_name') if node.params else None
                if node_conn_name and payload.get('connection_name') == node_conn_name:
                    emitting_node_id = node_id
                    break
                if node_ds_name and payload.get('datasource_name') == node_ds_name:
                    emitting_node_id = node_id
                    break
                if payload.get('datasource_name') == node_id:
                    emitting_node_id = node_id
                    break

            if not emitting_node_id:
                BBLogger.log(f"No emitting pipeline node matched for payload: {payload}")
                return

            BBLogger.log(f"Redis event mapped to pipeline node '{emitting_node_id}'")

            # By default ignore monitoring_started
            event_type = payload.get('event_type')
            if event_type == 'monitoring_started' and not self.nodes[emitting_node_id].params.get('allow_monitoring_started', False):
                BBLogger.log(f"Ignoring 'monitoring_started' event for node '{emitting_node_id}'")
                return

            downstream = [n_id for n_id, n in self.nodes.items() if emitting_node_id in n.dependencies]
            BBLogger.log(f"Resolved downstream nodes for '{emitting_node_id}': {downstream}")

            for dst in downstream:
                try:
                    BBLogger.log(f"Handing off payload to downstream node '{dst}': {payload}")
                    self._trigger_node(dst, payload)
                    BBLogger.log(f"Payload handed off to '{dst}' successfully")
                except Exception as e:
                    BBLogger.log(f"Error invoking downstream node '{dst}': {e}")

        except Exception as e:
            BBLogger.log(f"Error processing Redis payload: {e}")


class SubjectivePipelineDataSource(SubjectiveDataSource):
    """
    A data source that manages and executes pipelines defined in JSON.

    This allows pipelines to be:
    - Managed from the UI
    - Started/stopped like any data source
    - Configured using connection metadata
    - Defined declaratively without Python code
    """

    def __init__(self, name=None, session=None, dependency_data_sources=None, subscribers=None, params=None):
        super().__init__(
            name=name,
            session=session,
            dependency_data_sources=dependency_data_sources or [],
            subscribers=subscribers,
            params=params
        )

        self.pipeline = None
        self.pipeline_config = None
        self.pipeline_name = self.params.get('pipeline_name', 'Pipeline')

        BBLogger.log(f"SubjectivePipelineDataSource initialized: {self.pipeline_name}")

    def fetch(self):
        """
        Load and start the pipeline from configuration.
        """
        try:
            BBLogger.log("Starting pipeline data source")

            # Load pipeline configuration
            self._load_pipeline_config()

            # Build the pipeline from config
            self._build_pipeline_from_config()

            # Start the pipeline
            BBLogger.log(f"Starting pipeline: {self.pipeline_name}")
            self.pipeline.start()

            BBLogger.log(f"Pipeline {self.pipeline_name} is running")

            # Keep running (for real-time pipelines)
            import time
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                BBLogger.log("Pipeline interrupted by user")
                self.stop()

        except Exception as e:
            BBLogger.log(f"Error in pipeline data source: {e}")
            raise

    def stop(self):
        """
        Stop the pipeline.
        """
        if self.pipeline:
            BBLogger.log(f"Stopping pipeline: {self.pipeline_name}")
            self.pipeline.stop()
            BBLogger.log(f"Pipeline {self.pipeline_name} stopped")

    def get_icon(self):
        """
        Return SVG icon for pipeline data source.
        """
        return '''<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <rect x="3" y="4" width="6" height="6" rx="1" fill="#4F46E5"/>
            <rect x="15" y="4" width="6" height="6" rx="1" fill="#4F46E5"/>
            <rect x="9" y="14" width="6" height="6" rx="1" fill="#4F46E5"/>
            <path d="M6 10 L12 14 M18 10 L12 14" stroke="#4F46E5" stroke-width="2"/>
        </svg>'''

    def get_connection_data(self):
        """
        Return connection configuration for pipeline data source.
        """
        return {
            "connection_type": "PIPELINE",
            "fields": [
                {
                    "name": "pipeline_name",
                    "type": "text",
                    "label": "Pipeline Name",
                    "description": "Name for this pipeline",
                    "required": True
                },
                {
                    "name": "pipeline_json",
                    "type": "textarea",
                    "label": "Pipeline Definition (JSON)",
                    "description": "JSON configuration defining the pipeline",
                    "required": True
                },
                {
                    "name": "pipeline_json_path",
                    "type": "text",
                    "label": "Pipeline JSON File Path (Alternative)",
                    "description": "Path to JSON file with pipeline definition",
                    "required": False
                }
            ]
        }

    def _load_pipeline_config(self):
        """
        Load pipeline configuration from params.
        """
        # Try to load from inline JSON first (support pipeline_json and pipeline_data)
        pipeline_json = self.params.get('pipeline_json') or self.params.get('pipeline_data')
        if pipeline_json:
            try:
                if isinstance(pipeline_json, str):
                    self.pipeline_config = json.loads(pipeline_json)
                else:
                    self.pipeline_config = pipeline_json
                BBLogger.log("Loaded pipeline config from inline JSON/data")
                return
            except json.JSONDecodeError as e:
                BBLogger.log(f"Error parsing inline pipeline JSON: {e}")
                raise ValueError(f"Invalid pipeline JSON: {e}")

        # Try to load from file path (support both pipeline_json_path and pipeline_file)
        pipeline_json_path = self.params.get('pipeline_json_path') or self.params.get('pipeline_file')
        if pipeline_json_path:
            try:
                with open(pipeline_json_path, 'r', encoding='utf-8') as f:
                    self.pipeline_config = json.load(f)
                BBLogger.log(f"Loaded pipeline config from file: {pipeline_json_path}")
                return
            except (FileNotFoundError, json.JSONDecodeError) as e:
                BBLogger.log(f"Error loading pipeline from file: {e}")
                raise ValueError(f"Could not load pipeline from file: {e}")

        raise ValueError("No pipeline configuration provided (pipeline_json, pipeline_json_path, or pipeline_file)")

    def _build_pipeline_from_config(self):
        """
        Build the pipeline from the loaded configuration.
        """
        if not self.pipeline_config:
            raise ValueError("No pipeline configuration loaded")

        # Validate config structure
        if 'nodes' not in self.pipeline_config:
            raise ValueError("Pipeline config must have 'nodes' key")

        # Create the pipeline
        pipeline_name = self.pipeline_config.get('name', self.pipeline_name)
        self.pipeline = SubjectiveDataSourcePipeline(name=pipeline_name)

        BBLogger.log(f"Building pipeline from config: {pipeline_name}")

        # Add each node
        nodes = self.pipeline_config['nodes']
        for node_config in nodes:
            self._add_node_from_config(node_config)

        BBLogger.log(f"Pipeline built with {len(nodes)} nodes")

    def _add_node_from_config(self, node_config: Dict[str, Any]):
        """
        Add a node to the pipeline from configuration.

        Expected node_config format:
        {
            "node_id": "unique_id",
            "module": "package.module",
            "class": "DataSourceClassName",
            "params": {...},
            "dependencies": ["other_node_id"],
            "filter": "python_expression",
            "transform": "python_expression"
        }
        """
        node_id = node_config.get('node_id')
        if not node_id:
            raise ValueError("Each node must have a 'node_id'")

        module_path = node_config.get('module')
        class_name = node_config.get('class')

        if not class_name:
            raise ValueError(f"Node '{node_id}' must specify 'class'")

        # Load the data source class using the shared importer
        # This will search data_source_addons, data_source_installed_plugins, and abstract package
        try:
            BBLogger.log(f"Importing data source class '{class_name}' for node '{node_id}'")
            data_source_class = import_datasource_class(class_name, project_root=os.getcwd())
            BBLogger.log(f"Successfully imported class '{class_name}'")
        except ImportError as e:
            BBLogger.log(f"Failed to load class {class_name}: {e}")
            raise ValueError(f"Could not load data source class '{class_name}': {e}")

        # Get parameters
        params = node_config.get('params', {})

        # Get dependencies
        dependencies = node_config.get('dependencies', [])

        # Create filter function if specified
        filter_fn = None
        filter_expr = node_config.get('filter')
        if filter_expr:
            filter_fn = self._create_filter_function(filter_expr)

        # Create transform function if specified
        transform_fn = None
        transform_expr = node_config.get('transform')
        if transform_expr:
            transform_fn = self._create_transform_function(transform_expr)

        # Add the node
        self.pipeline.add_node(
            node_id=node_id,
            data_source_class=data_source_class,
            params=params,
            dependencies=dependencies,
            filter_fn=filter_fn,
            transform_fn=transform_fn
        )

        BBLogger.log(f"Added node '{node_id}' to pipeline")

    def _create_filter_function(self, filter_expr: str):
        """
        Create a filter function from a string expression.

        The expression should be a Python expression that evaluates to bool.
        The variable 'data' is available in the expression.

        Examples:
            "data.get('event_type') == 'created'"
            "str(data.get('path', '')).endswith('.mp4')"
        """
        def filter_fn(data):
            try:
                # Make 'data' available to the expression
                return eval(filter_expr, {"__builtins__": {}}, {"data": data, "str": str, "int": int, "float": float})
            except Exception as e:
                BBLogger.log(f"Error evaluating filter expression '{filter_expr}': {e}")
                return False

        return filter_fn

    def _create_transform_function(self, transform_expr: str):
        """
        Create a transform function from a string expression.

        The expression should be a Python expression that returns the transformed data.
        The variable 'data' is available in the expression.

        Examples:
            "data.get('path')"
            "{'video_path': data.get('path'), 'timestamp': data.get('timestamp')}"
        """
        def transform_fn(data):
            try:
                # Make 'data' available to the expression
                return eval(transform_expr, {"__builtins__": {}}, {"data": data, "str": str, "int": int, "float": float})
            except Exception as e:
                BBLogger.log(f"Error evaluating transform expression '{transform_expr}': {e}")
                return data

        return transform_fn

    def get_data_source_type_name(self):
        """
        Returns the name of the data source type.
        """
        return "Pipeline"
