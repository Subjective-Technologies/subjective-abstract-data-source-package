"""
SubjectiveDataSourcePipeline

A pipeline orchestration system for coordinating multiple data sources,
managing dependencies, and transforming data flow between sources.
"""

from typing import Dict, List, Optional, Callable, Any
from brainboost_data_source_logger_package.BBLogger import BBLogger
import importlib
import os


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
        """
        Initialize a pipeline node.

        Args:
            node_id: Unique identifier for this node
            data_source_class: The data source class (not instance)
            params: Parameters to pass to the data source constructor
            dependencies: List of node_ids this node depends on
            transform_fn: Optional function to transform data before passing to this node
            filter_fn: Optional function to filter which updates trigger this node
        """
        self.node_id = node_id
        self.data_source_class = data_source_class
        self.params = params or {}
        self.dependencies = dependencies or []
        self.transform_fn = transform_fn
        self.filter_fn = filter_fn
        self.instance = None

    def should_process(self, data: Any) -> bool:
        """Check if this node should process the given data."""
        if self.filter_fn is None:
            return True
        try:
            return self.filter_fn(data)
        except Exception as e:
            BBLogger.log(f"Filter function error in node {self.node_id}: {e}")
            return False

    def transform_data(self, data: Any) -> Any:
        """Transform the data before processing."""
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
        """
        Called when the dependency data source sends an update.
        Filters, transforms, and processes the data.
        """
        try:
            BBLogger.log(f"PipelineAdapter for {self.pipeline_node.node_id} received update: {data}")

            # Check if this node should process this data
            if not self.pipeline_node.should_process(data):
                BBLogger.log(f"Node {self.pipeline_node.node_id} filtered out update")
                return

            # Transform the data
            transformed_data = self.pipeline_node.transform_data(data)

            # Trigger the data source with the transformed data
            self.pipeline._trigger_node(self.pipeline_node.node_id, transformed_data)

        except Exception as e:
            BBLogger.log(f"Error in PipelineAdapter for {self.pipeline_node.node_id}: {e}")


class SubjectiveDataSourcePipeline:
    """
    Pipeline orchestrator for coordinating multiple data sources.
    Manages dependencies, data flow, and transformations between sources.
    """

    def __init__(self, name: str = "DataPipeline"):
        """
        Initialize the pipeline.

        Args:
            name: Name for this pipeline instance
        """
        self.name = name
        self.nodes: Dict[str, PipelineNode] = {}
        self.adapters: Dict[str, List[PipelineAdapter]] = {}
        self._started = False

        BBLogger.log(f"Pipeline '{self.name}' initialized")

    def add_node(
        self,
        node_id: str,
        data_source_class,
        params: Optional[Dict[str, Any]] = None,
        dependencies: Optional[List[str]] = None,
        transform_fn: Optional[Callable] = None,
        filter_fn: Optional[Callable] = None
    ) -> 'SubjectiveDataSourcePipeline':
        """
        Add a node to the pipeline.

        Args:
            node_id: Unique identifier for this node
            data_source_class: The data source class (not instance)
            params: Parameters to pass to the data source constructor
            dependencies: List of node_ids this node depends on
            transform_fn: Optional function to transform data: (data) -> transformed_data
            filter_fn: Optional function to filter updates: (data) -> bool

        Returns:
            Self for method chaining
        """
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
        BBLogger.log(f"Added node '{node_id}' to pipeline '{self.name}'")

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
        """
        Add a node by dynamically loading a data source class from a module.

        Args:
            node_id: Unique identifier for this node
            module_path: Path to the Python module (e.g., 'package.module')
            class_name: Name of the data source class
            params: Parameters to pass to the data source constructor
            dependencies: List of node_ids this node depends on
            transform_fn: Optional function to transform data
            filter_fn: Optional function to filter updates

        Returns:
            Self for method chaining
        """
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
            BBLogger.log(f"Pipeline '{self.name}' already built")
            return

        BBLogger.log(f"Building pipeline '{self.name}'")

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

                # Create the data source instance
                node.instance = node.data_source_class(
                    name=node_id,
                    dependency_data_sources=dependency_instances,
                    params=node.params
                )

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
        BBLogger.log(f"Pipeline '{self.name}' built successfully")

    def start(self):
        """
        Start the pipeline by starting all data sources.
        """
        if not self._started:
            self.build()

        BBLogger.log(f"Starting pipeline '{self.name}'")

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
                    import threading
                    thread = threading.Thread(
                        target=node.instance.fetch,
                        name=f"Pipeline-{self.name}-{node_id}",
                        daemon=True
                    )
                    thread.start()

                started.add(node_id)
                BBLogger.log(f"Started node '{node_id}'")

            except Exception as e:
                BBLogger.log(f"Failed to start node '{node_id}': {e}")
                raise

        # Start all nodes
        for node_id in self.nodes:
            start_node(node_id)

        BBLogger.log(f"Pipeline '{self.name}' started successfully")

    def stop(self):
        """
        Stop the pipeline by stopping all data sources.
        """
        BBLogger.log(f"Stopping pipeline '{self.name}'")

        for node_id, node in self.nodes.items():
            try:
                if node.instance and hasattr(node.instance, 'stop'):
                    node.instance.stop()
                    BBLogger.log(f"Stopped node '{node_id}'")
            except Exception as e:
                BBLogger.log(f"Error stopping node '{node_id}': {e}")

        self._started = False
        BBLogger.log(f"Pipeline '{self.name}' stopped")

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
            # Otherwise, call update to notify its subscribers
            elif hasattr(node.instance, 'update'):
                node.instance.update(data)
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
