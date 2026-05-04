# SubjectiveDataSourcePipeline

## What is it?

The **SubjectiveDataSourcePipeline** is a powerful orchestration system that lets you easily connect multiple data sources together in a pipeline. Think of it like UNIX pipes, but for data sources!

## Why use it?

Instead of manually wiring up data sources and managing their dependencies, the pipeline handles:

- ✅ **Automatic dependency management** - Data sources start in the right order
- ✅ **Easy data flow** - Updates from one source automatically trigger the next
- ✅ **Filtering** - Only process the data you care about
- ✅ **Transformation** - Modify data as it flows between sources
- ✅ **Validation** - Catches circular dependencies and configuration errors
- ✅ **Clean shutdown** - Properly stops all data sources

## Quick Example

Want to automatically transcribe videos when they're added to a folder? Here's how:

```python
from subjective_abstract_data_source_package import SubjectiveDataSourcePipeline
from SubjectiveLocalFolderMonitorDataSource import SubjectiveLocalFolderMonitorDataSource
from SubjectiveTranscribeLocalVideoDataSource import SubjectiveTranscribeLocalVideoDataSource

# Create pipeline
pipeline = SubjectiveDataSourcePipeline(name="AutoTranscribe")

# Watch a folder
pipeline.add_node(
    node_id="monitor",
    data_source_class=SubjectiveLocalFolderMonitorDataSource,
    params={"url": r"C:\videos"}
)

# Transcribe new videos
pipeline.add_node(
    node_id="transcriber",
    data_source_class=SubjectiveTranscribeLocalVideoDataSource,
    params={"context_dir": r"C:\transcriptions"},
    dependencies=["monitor"],  # Transcriber depends on monitor
    filter_fn=lambda data: data.get('event_type') == 'created'
)

# Start!
pipeline.build()
pipeline.start()
```

That's it! Now when you add a `.mp4` or `.mkv` file to `C:\videos`, it will automatically be transcribed.

## How It Works

### 1. Create the Pipeline

```python
pipeline = SubjectiveDataSourcePipeline(name="MyPipeline")
```

### 2. Add Nodes (Data Sources)

```python
pipeline.add_node(
    node_id="unique_name",           # Unique identifier
    data_source_class=MyDataSource,  # The data source class
    params={"key": "value"},         # Configuration parameters
    dependencies=["other_node"],     # Which nodes this depends on (optional)
    filter_fn=my_filter,             # Filter updates (optional)
    transform_fn=my_transform        # Transform data (optional)
)
```

### 3. Build and Start

```python
pipeline.build()  # Validates and instantiates everything
pipeline.start()  # Starts all data sources in dependency order
```

## Real-World Use Case: Video Transcription

**Problem**: You have a folder where video recordings are saved. You want to automatically transcribe any new videos.

**Without Pipeline**:
```python
# Manual setup - messy and error-prone
monitor = SubjectiveLocalFolderMonitorDataSource(params={"url": folder})
transcriber = SubjectiveTranscribeLocalVideoDataSource(params={"context_dir": output})

# Manually subscribe
class VideoAdapter:
    def update(self, data):
        if data.get('event_type') == 'created':
            path = data.get('path')
            if path.endswith(('.mp4', '.mkv')):
                transcriber.process_input(data)

adapter = VideoAdapter()
monitor.subscribe(adapter)
monitor.start()
# ... lots more manual wiring
```

**With Pipeline**:
```python
# Clean and declarative
pipeline = SubjectiveDataSourcePipeline(name="AutoTranscribe")

pipeline.add_node("monitor", SubjectiveLocalFolderMonitorDataSource,
                  params={"url": folder})

pipeline.add_node("transcriber", SubjectiveTranscribeLocalVideoDataSource,
                  params={"context_dir": output},
                  dependencies=["monitor"],
                  filter_fn=lambda d: d.get('event_type') == 'created')

pipeline.start()
```

## Key Features

### Dependencies

Specify which data sources depend on others:

```python
pipeline.add_node("source", SourceDataSource)
pipeline.add_node("processor", ProcessorDataSource, dependencies=["source"])
```

When `source` sends updates, `processor` receives them automatically.

### Filters

Only process updates that match your criteria:

```python
def only_videos(data):
    return data.get('path', '').endswith(('.mp4', '.mkv'))

pipeline.add_node("transcriber", TranscriberDataSource,
                  dependencies=["monitor"],
                  filter_fn=only_videos)
```

### Transforms

Modify data before passing it to the next node:

```python
def extract_path(data):
    return data.get('path') if isinstance(data, dict) else data

pipeline.add_node("processor", ProcessorDataSource,
                  dependencies=["source"],
                  transform_fn=extract_path)
```

## Making Your Data Source Pipeline-Ready

To make a data source work in a pipeline, add a `process_input()` method:

```python
class MyDataSource(SubjectiveDataSource):

    def process_input(self, data):
        """Called when receiving data from a dependency"""
        # Process the input
        result = self.do_something(data)

        # Notify subscribers (if this node has dependents)
        self.update(result)

    def fetch(self):
        """Normal fetch for standalone use"""
        # Your regular fetch logic
        pass
```

## API Reference

### SubjectiveDataSourcePipeline

#### `__init__(name: str)`
Create a new pipeline.

#### `add_node(node_id, data_source_class, params, dependencies, filter_fn, transform_fn)`
Add a data source to the pipeline.

- **node_id**: Unique identifier (str)
- **data_source_class**: The data source class (not an instance)
- **params**: Configuration dict for the data source
- **dependencies**: List of node_id strings this node depends on
- **filter_fn**: Function that takes data and returns bool
- **transform_fn**: Function that takes data and returns transformed data

#### `build()`
Validate dependencies and instantiate all data sources.

#### `start()`
Start all data sources in dependency order. Calls `build()` if not already built.

#### `stop()`
Stop all data sources.

#### `get_node_instance(node_id)`
Get the data source instance for a specific node.

## Examples

See the `examples/` directory:

- **simple_pipeline_example.py** - Minimal example showing the basics
- **video_transcription_pipeline_example.py** - Complete example with documentation

## Documentation

See **PIPELINE_GUIDE.md** for comprehensive documentation including:
- Detailed API reference
- Advanced features
- Best practices
- Troubleshooting guide

## Benefits Summary

| Without Pipeline | With Pipeline |
|-----------------|---------------|
| Manual dependency tracking | Automatic dependency management |
| Manual subscription logic | Declarative configuration |
| Scattered filtering code | Centralized filter functions |
| Error-prone wiring | Validated connections |
| Complex shutdown | One-line cleanup |

## Getting Started

1. Import the pipeline:
   ```python
   from subjective_abstract_data_source_package import SubjectiveDataSourcePipeline
   ```

2. Create a pipeline:
   ```python
   pipeline = SubjectiveDataSourcePipeline(name="MyPipeline")
   ```

3. Add your data sources:
   ```python
   pipeline.add_node("node1", DataSource1, params={...})
   pipeline.add_node("node2", DataSource2, dependencies=["node1"])
   ```

4. Start it:
   ```python
   pipeline.start()
   ```

That's it! Your data sources are now connected and working together.
