# Pipeline JSON Schema Documentation

## Overview

Pipelines can be defined declaratively using JSON, allowing them to be:
- Managed from the UI
- Stored in connection metadata
- Created without writing Python code
- Easily shared and version controlled

## JSON Schema

```json
{
  "name": "PipelineName",
  "description": "Optional description of what this pipeline does",
  "nodes": [
    {
      "node_id": "unique_identifier",
      "module": "module.path.to.datasource",
      "class": "DataSourceClassName",
      "params": {
        "param1": "value1",
        "param2": "value2"
      },
      "dependencies": ["other_node_id"],
      "filter": "optional_python_expression",
      "transform": "optional_python_expression"
    }
  ]
}
```

## Field Definitions

### Pipeline Level

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Name of the pipeline |
| `description` | string | No | Human-readable description |
| `nodes` | array | Yes | Array of node definitions |

### Node Level

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `node_id` | string | Yes | Unique identifier for this node |
| `module` | string | Yes | Python module path containing the data source class |
| `class` | string | Yes | Name of the data source class |
| `params` | object | No | Parameters passed to the data source constructor |
| `dependencies` | array | No | Array of node_id strings this node depends on |
| `filter` | string | No | Python expression to filter updates (returns bool) |
| `transform` | string | No | Python expression to transform data |

## Filter Expressions

Filter expressions are Python expressions that determine whether a node should process an update.

**Available Variables:**
- `data` - The update data from the dependency

**Available Functions:**
- `str()`, `int()`, `float()` - Type conversions

**Examples:**

```json
"filter": "data.get('event_type') == 'created'"
```

```json
"filter": "str(data.get('path', '')).endswith('.mp4')"
```

```json
"filter": "data.get('event_type') == 'created' and str(data.get('path', '')).endswith(('.mp4', '.mkv'))"
```

## Transform Expressions

Transform expressions modify data before passing it to the next node.

**Available Variables:**
- `data` - The update data from the dependency

**Available Functions:**
- `str()`, `int()`, `float()` - Type conversions

**Examples:**

```json
"transform": "data.get('path')"
```

```json
"transform": "{'video_path': data.get('path'), 'timestamp': data.get('timestamp')}"
```

```json
"transform": "data"
```

## Complete Example: Video Transcription Pipeline

```json
{
  "name": "VideoTranscriptionPipeline",
  "description": "Automatically transcribe video files when added to a monitored folder",
  "nodes": [
    {
      "node_id": "folder_monitor",
      "module": "SubjectiveLocalFolderMonitorDataSource",
      "class": "SubjectiveLocalFolderMonitorDataSource",
      "params": {
        "url": "C:\\videos",
        "connection_name": "VideoMonitor"
      },
      "dependencies": []
    },
    {
      "node_id": "video_transcriber",
      "module": "SubjectiveTranscribeLocalVideoDataSource",
      "class": "SubjectiveTranscribeLocalVideoDataSource",
      "params": {
        "context_dir": "C:\\transcriptions",
        "whisper_model_size": "base"
      },
      "dependencies": ["folder_monitor"],
      "filter": "data.get('event_type') == 'created' and str(data.get('path', '')).endswith(('.mp4', '.mkv'))"
    }
  ]
}
```

## Using JSON Pipelines

### Method 1: Inline JSON in Connection Metadata

```python
from subjective_abstract_data_source_package import SubjectivePipelineDataSource

pipeline_json = '''
{
  "name": "MyPipeline",
  "nodes": [...]
}
'''

pipeline_ds = SubjectivePipelineDataSource(
    params={
        "pipeline_name": "MyPipeline",
        "pipeline_json": pipeline_json
    }
)

pipeline_ds.fetch()
```

### Method 2: JSON File Path

```python
pipeline_ds = SubjectivePipelineDataSource(
    params={
        "pipeline_name": "MyPipeline",
        "pipeline_json_path": "path/to/pipeline.json"
    }
)

pipeline_ds.fetch()
```

### Method 3: From UI (Connection Metadata)

In your UI, when creating a Pipeline data source connection:

1. Select connection type: **PIPELINE**
2. Fill in fields:
   - **Pipeline Name**: `VideoTranscription`
   - **Pipeline Definition (JSON)**: Paste the JSON configuration
3. Save and start the data source

## Module Path Examples

When specifying the `module` field, use the full module path:

| Data Source | Module Path |
|-------------|-------------|
| Local Folder Monitor | `SubjectiveLocalFolderMonitorDataSource` |
| Transcribe Local Video | `SubjectiveTranscribeLocalVideoDataSource` |
| Custom Plugin | `my_plugin_package.MyDataSource` |

## Validation

The pipeline validates:

✅ All required fields are present
✅ Module and class can be imported
✅ Dependencies reference existing nodes
✅ No circular dependencies
✅ Filter and transform expressions are valid Python

## Best Practices

### 1. Use Descriptive Node IDs
```json
// Good
"node_id": "folder_monitor"

// Bad
"node_id": "node1"
```

### 2. Add Comments (Using Description)
```json
{
  "name": "MyPipeline",
  "description": "This pipeline monitors folder X and processes files using Y",
  "nodes": [...]
}
```

### 3. Keep Filter Expressions Simple
```json
// Good
"filter": "data.get('event_type') == 'created'"

// Less readable
"filter": "data.get('event_type') == 'created' and data.get('path') and len(data.get('path')) > 0 and ..."
```

### 4. Use Absolute Paths in Params
```json
"params": {
  "url": "C:\\absolute\\path\\to\\folder"
}
```

### 5. Escape Backslashes in Windows Paths
```json
// Correct
"url": "C:\\videos\\folder"

// Wrong
"url": "C:\videos\folder"
```

## Security Considerations

**Filter and Transform Expressions:**
- Executed using `eval()` with restricted builtins
- Only safe functions are available: `str()`, `int()`, `float()`
- Access to `data` variable only
- **Do NOT include user-provided expressions from untrusted sources**

## Troubleshooting

### "Could not load data source class"
- Check that the `module` path is correct
- Ensure the module is installed and importable
- Verify the `class` name matches exactly

### "Pipeline config must have 'nodes' key"
- JSON must have a top-level `"nodes"` array

### "Each node must have a 'node_id'"
- Every node in the `nodes` array needs a unique `node_id`

### "Invalid pipeline JSON"
- Check JSON syntax (commas, brackets, quotes)
- Use a JSON validator to verify format

### Filter/Transform Not Working
- Check expression syntax
- Verify `data` variable is used correctly
- Test expressions with sample data first

## Alternative Format: YAML (Future)

YAML support can be added for more readable configurations:

```yaml
name: VideoTranscriptionPipeline
description: Automatically transcribe videos
nodes:
  - node_id: folder_monitor
    module: SubjectiveLocalFolderMonitorDataSource
    class: SubjectiveLocalFolderMonitorDataSource
    params:
      url: C:\videos
      connection_name: VideoMonitor

  - node_id: video_transcriber
    module: SubjectiveTranscribeLocalVideoDataSource
    class: SubjectiveTranscribeLocalVideoDataSource
    params:
      context_dir: C:\transcriptions
      whisper_model_size: base
    dependencies:
      - folder_monitor
    filter: data.get('event_type') == 'created'
```

## Examples Directory

See `examples/` for complete working examples:
- `video_transcription_pipeline.json` - Video transcription pipeline
