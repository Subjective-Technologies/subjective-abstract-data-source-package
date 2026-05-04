# SubjectiveDataSourcePipeline - New Implementation

## Overview

The `SubjectiveDataSourcePipeline` class has been redesigned to inherit from `SubjectiveDataSource`, making pipelines first-class data sources that can be used anywhere a `SubjectiveDataSource` is expected.

## Key Changes

### 1. **Inheritance from SubjectiveDataSource**

**Before:**
```python
class SubjectiveDataSourcePipeline:
    def __init__(self, name: str = "DataPipeline"):
        self.name = name
        # ...
```

**After:**
```python
class SubjectiveDataSourcePipeline(SubjectiveDataSource):
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
        # ...
```

### 2. **Load from .pipe Files**

The pipeline can now be instantiated from a `.pipe` (JSON) file:

```python
my_pipeline_ds = SubjectiveDataSourcePipeline(
    name="VideoTranscription",
    params={
        "pipeline_file": "path/to/pipeline.pipe"
    }
)
```

The `.pipe` file format (JSON):
```json
{
  "name": "video_transcript_pipeline",
  "description": "Pipeline description",
  "nodes": [
    {
      "node_id": "monitor",
      "module": "SubjectiveLocalFolderMonitorDataSource",
      "class": "SubjectiveLocalFolderMonitorDataSource",
      "params": {
        "url": "C:\\Videos"
      }
    },
    {
      "node_id": "transcriber",
      "module": "SubjectiveTranscribeLocalVideoDataSource",
      "class": "SubjectiveTranscribeLocalVideoDataSource",
      "params": {
        "context_dir": "C:\\context"
      },
      "dependencies": ["monitor"],
      "filter": "data.get('event_type') == 'created'"
    }
  ]
}
```

### 3. **Standard Data Source Interface**

The pipeline now implements all required `SubjectiveDataSource` abstract methods:

- `fetch()` - Starts the pipeline and runs until stopped
- `get_icon()` - Returns SVG icon for UI
- `get_connection_data()` - Returns connection metadata

### 4. **Works with Dependencies and Subscribers**

Since it's now a proper `SubjectiveDataSource`:

```python
# Use as a dependency
another_ds = SomeOtherDataSource(
    dependency_data_sources=[my_pipeline_ds]
)

# Subscribe to updates
class MySubscriber:
    def update(self, data):
        print(f"Received: {data}")

my_pipeline_ds.subscribe(MySubscriber())
```

## Usage Examples

### Example 1: Load from .pipe File

```python
from subjective_abstract_data_source_package import SubjectiveDataSourcePipeline

# Load pipeline from file
pipeline = SubjectiveDataSourcePipeline(
    name="MyPipeline",
    params={
        "pipeline_file": "com_subjective_userdata/com_subjective_pipelines/video_transcript_pipeline.pipe"
    }
)

# Run like any data source
pipeline.fetch()
```

### Example 2: Programmatic Configuration

```python
from subjective_abstract_data_source_package import SubjectiveDataSourcePipeline
from SomeDataSource import SomeDataSource
from AnotherDataSource import AnotherDataSource

# Create pipeline
pipeline = SubjectiveDataSourcePipeline(name="MyPipeline")

# Add nodes
pipeline.add_node(
    node_id="source",
    data_source_class=SomeDataSource,
    params={"url": "/path/to/data"}
)

pipeline.add_node(
    node_id="processor",
    data_source_class=AnotherDataSource,
    params={"output_dir": "/path/to/output"},
    dependencies=["source"],
    filter_fn=lambda data: data.get('type') == 'video'
)

# Run
pipeline.fetch()
```

### Example 3: Pipeline with Dependencies

```python
# Pipeline can now have dependencies like any data source
upstream_ds = SomeDataSource(params={"url": "/data"})

pipeline = SubjectiveDataSourcePipeline(
    name="DependentPipeline",
    dependency_data_sources=[upstream_ds],
    params={...}
)

pipeline.fetch()
```

### Example 4: Composable Pipelines

```python
# Create a sub-pipeline
video_processing = SubjectiveDataSourcePipeline(name="VideoProcessing")
video_processing.add_node(...)
video_processing.add_node(...)

# Use it as a dependency in another pipeline
master_pipeline = SubjectiveDataSourcePipeline(
    name="MasterPipeline",
    dependency_data_sources=[video_processing]
)

master_pipeline.fetch()
```

## Benefits

1. **Uniform Interface**: Pipelines work exactly like any other data source
2. **Composability**: Pipelines can depend on other pipelines
3. **File-Based Configuration**: Load pipelines from `.pipe` files for easy configuration
4. **Full Feature Set**: Inherits all SubjectiveDataSource features:
   - Progress tracking
   - Context output
   - Subscriber notifications
   - Dependency management

## Migration Notes

If you're using the old `SubjectiveDataSourcePipeline` class:

### Old Way:
```python
pipeline = SubjectiveDataSourcePipeline(name="MyPipeline")
pipeline.add_node(...)
pipeline.build()
pipeline.start()

# Keep running
while True:
    time.sleep(1)
```

### New Way:
```python
pipeline = SubjectiveDataSourcePipeline(name="MyPipeline")
pipeline.add_node(...)
pipeline.fetch()  # Handles build, start, and running
```

The `build()` and `start()` methods still exist but are called automatically by `fetch()`.

## Architecture

```
SubjectiveDataSource (Abstract Base)
    ↑
    |
    | inherits from
    |
SubjectiveDataSourcePipeline
    |
    | contains
    |
    ├─── PipelineNode (wrapper for data sources)
    ├─── PipelineAdapter (connects dependencies)
    └─── nodes: Dict[str, PipelineNode]
```

## Real-World Use Case: Video Transcription

Your video transcription pipeline:

**File**: `com_subjective_userdata/com_subjective_pipelines/video_transcript_pipeline.pipe`

```json
{
  "name": "video_transcript_pipeline",
  "nodes": [
    {
      "node_id": "local_videos_monitor",
      "module": "SubjectiveLocalFolderMonitorDataSource",
      "class": "SubjectiveLocalFolderMonitorDataSource",
      "params": {
        "url": "C:\\Users\\pablo\\Videos"
      }
    },
    {
      "node_id": "video_transcriber",
      "module": "SubjectiveTranscribeLocalVideoDataSource",
      "class": "SubjectiveTranscribeLocalVideoDataSource",
      "params": {
        "context_dir": "C:\\context"
      },
      "dependencies": ["local_videos_monitor"]
    }
  ]
}
```

**Usage**:
```python
from subjective_abstract_data_source_package import SubjectiveDataSourcePipeline

# Load pipeline from file
pipeline = SubjectiveDataSourcePipeline(
    params={
        "pipeline_file": "com_subjective_userdata/com_subjective_pipelines/video_transcript_pipeline.pipe"
    }
)

# Now it works like any data source
pipeline.fetch()
```

**Flow**:
1. Video folder monitor detects new `.mp4` file
2. Sends Redis notification → triggers desktop notification
3. Pipeline adapter receives the notification
4. Triggers video transcriber node with video path
5. Transcriber generates transcript and saves as context
6. Pipeline can notify subscribers of the completed transcription

## Implementation Details

### Key Methods

#### `fetch()`
Implements the abstract method from `SubjectiveDataSource`. Builds and starts the pipeline, then runs until interrupted.

#### `_load_from_pipe_file(file_path)`
Loads and parses a `.pipe` (JSON) file and builds the pipeline from the configuration.

#### `add_node(...)`
Adds a node to the pipeline. Nodes are wrappers around data source instances.

#### `build()`
Instantiates all data sources and sets up dependency connections via adapters.

#### `start()`
Starts all nodes in dependency order (dependencies first).

#### `_trigger_node(node_id, data)`
Called by PipelineAdapters when dependency data arrives. Triggers the target node with the transformed/filtered data.

### PipelineAdapter

The adapter pattern is used to connect nodes:

```
DataSource A (dependency)
    ↓ update()
PipelineAdapter
    ↓ filter + transform
    ↓ _trigger_node()
DataSource B (dependent)
```

When a dependency emits data via `update()`, the adapter:
1. Checks the filter (if any)
2. Applies transformation (if any)
3. Triggers the dependent node with the processed data

## Testing

Test file: `examples/test_new_pipeline_as_datasource.py`

Run with:
```bash
cd C:\brainboost\brainboost_data\data_source\subjective-abstract-data-source-package\examples
python test_new_pipeline_as_datasource.py
```

## Next Steps

1. **Replace old implementation**: Rename `SubjectiveDataSourcePipeline.py` to `SubjectiveDataSourcePipeline_OLD.py`
2. **Use new implementation**: Rename `SubjectiveDataSourcePipeline_NEW.py` to `SubjectiveDataSourcePipeline.py`
3. **Update imports**: Existing code using `SubjectivePipelineDataSource` can continue to work
4. **Test thoroughly**: Run your video transcription pipeline with the new implementation
5. **Update UI**: If you have a UI for creating pipelines, it now works with standard data source interface

## Conclusion

The new implementation makes pipelines true first-class citizens in the data source ecosystem. They can be used anywhere a `SubjectiveDataSource` is expected, loaded from files, and composed with other pipelines for complex data processing workflows.
