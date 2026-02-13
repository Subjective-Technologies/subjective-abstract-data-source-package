# Data Source Pipeline Guide

## Overview

The `SubjectiveDataSourcePipeline` allows you to easily coordinate multiple data sources, define dependencies between them, and control data flow with filters and transformations.

## Key Concepts

### Pipeline
A container that manages multiple data sources and their relationships.

### Node
Represents a data source in the pipeline. Each node has:
- **node_id**: Unique identifier
- **data_source_class**: The data source class to instantiate
- **params**: Configuration parameters for the data source
- **dependencies**: List of other nodes this node depends on
- **filter_fn**: Optional function to filter which updates trigger this node
- **transform_fn**: Optional function to transform data before passing to this node

### Dependencies
When Node B depends on Node A:
- Node A starts first
- When Node A sends updates, Node B receives them
- Node B's `process_input(data)` method is called with the update data

## Basic Usage

```python
from subjective_abstract_data_source_package import SubjectiveDataSourcePipeline

# Create a pipeline
pipeline = SubjectiveDataSourcePipeline(name="MyPipeline")

# Add nodes
pipeline.add_node(
    node_id="source",
    data_source_class=MySourceDataSource,
    params={"url": "..."}
)

pipeline.add_node(
    node_id="processor",
    data_source_class=MyProcessorDataSource,
    params={"output_dir": "..."},
    dependencies=["source"]  # Depends on 'source' node
)

# Build and start
pipeline.build()
pipeline.start()
```

## Video Transcription Pipeline Example

This example monitors a folder for new video files and automatically transcribes them:

```python
from subjective_abstract_data_source_package import SubjectiveDataSourcePipeline
from SubjectiveLocalFolderMonitorDataSource import SubjectiveLocalFolderMonitorDataSource
from SubjectiveTranscribeLocalVideoDataSource import SubjectiveTranscribeLocalVideoDataSource

# Filter function: only process video file creation events
def is_video_file(data):
    if not isinstance(data, dict):
        return False

    event_type = data.get('event_type')
    if event_type != 'created':
        return False

    file_path = data.get('path', '')
    return file_path.endswith(('.mp4', '.mkv'))

# Create pipeline
pipeline = SubjectiveDataSourcePipeline(name="VideoTranscription")

# Add folder monitor
pipeline.add_node(
    node_id="monitor",
    data_source_class=SubjectiveLocalFolderMonitorDataSource,
    params={
        "url": r"C:\videos",
        "connection_name": "VideoMonitor"
    }
)

# Add video transcriber (depends on monitor)
pipeline.add_node(
    node_id="transcriber",
    data_source_class=SubjectiveTranscribeLocalVideoDataSource,
    params={
        "context_dir": r"C:\transcriptions",
        "whisper_model_size": "base"
    },
    dependencies=["monitor"],
    filter_fn=is_video_file
)

# Start the pipeline
pipeline.build()
pipeline.start()
```

## Filter Functions

Filter functions determine whether a node should process an update:

```python
def my_filter(data):
    """
    Args:
        data: The update data from the dependency

    Returns:
        True to process this update, False to ignore it
    """
    if not isinstance(data, dict):
        return False

    # Only process certain event types
    return data.get('event_type') == 'created'

pipeline.add_node(
    node_id="filtered_node",
    data_source_class=MyDataSource,
    dependencies=["source"],
    filter_fn=my_filter
)
```

## Transform Functions

Transform functions modify data before passing it to a node:

```python
def extract_path(data):
    """
    Args:
        data: The update data from the dependency

    Returns:
        The transformed data
    """
    if isinstance(data, dict):
        return data.get('path')
    return data

pipeline.add_node(
    node_id="transformed_node",
    data_source_class=MyDataSource,
    dependencies=["source"],
    transform_fn=extract_path
)
```

## Pipeline Methods

### `add_node()`
Add a data source to the pipeline.

```python
pipeline.add_node(
    node_id="my_node",
    data_source_class=MyDataSource,
    params={"key": "value"},
    dependencies=["other_node"],
    filter_fn=my_filter,
    transform_fn=my_transform
)
```

### `build()`
Instantiate all data sources and set up dependencies. Validates for circular dependencies.

```python
pipeline.build()
```

### `start()`
Start all data sources in dependency order. Automatically calls `build()` if not already built.

```python
pipeline.start()
```

### `stop()`
Stop all data sources.

```python
pipeline.stop()
```

### `get_node_instance()`
Get the data source instance for a specific node.

```python
transcriber = pipeline.get_node_instance("transcriber")
```

## Implementing Data Sources for Pipelines

To make a data source work in a pipeline, implement the `process_input(data)` method:

```python
class MyDataSource(SubjectiveDataSource):

    def process_input(self, data):
        """
        Called when this data source receives data from a dependency.

        Args:
            data: Input data from the dependency
        """
        # Process the input data
        result = self.process(data)

        # Notify subscribers with the result
        self.update(result)

    def fetch(self):
        """
        Standard fetch method for standalone operation.
        """
        # Regular fetch logic
        pass
```

## Advanced: Loading Data Sources Dynamically

You can load data sources from modules without importing them:

```python
pipeline.add_node_from_module(
    node_id="dynamic_node",
    module_path="my_package.my_module",
    class_name="MyDataSourceClass",
    params={"key": "value"}
)
```

## Pipeline Validation

The pipeline automatically validates:
- All dependencies exist
- No circular dependencies
- Proper node instantiation

If validation fails, an exception is raised with details.

## Best Practices

1. **Use meaningful node IDs**: Choose descriptive names like "folder_monitor" instead of "node1"

2. **Filter early**: Use filter functions to avoid unnecessary processing

3. **Keep transforms simple**: Transform functions should be lightweight

4. **Handle errors in data sources**: Implement proper error handling in `process_input()`

5. **Use dependency order**: Let the pipeline start nodes in the correct order

6. **Clean up resources**: Implement `stop()` methods in your data sources

## Troubleshooting

### "Circular dependency detected"
Check your dependencies - Node A cannot depend on Node B if Node B depends on Node A (directly or indirectly).

### "Node has no process_input method"
Implement `process_input(data)` in your data source class to handle pipeline inputs.

### Data source not receiving updates
- Check that dependencies are specified correctly
- Verify the dependency is calling `update()` or `send_notification()`
- Check filter functions aren't blocking updates

## Complete Example

See `examples/video_transcription_pipeline_example.py` for a complete working example.
