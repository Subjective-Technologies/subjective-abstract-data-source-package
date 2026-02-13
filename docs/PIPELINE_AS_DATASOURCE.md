# Pipelines as Data Sources

## Overview

**SubjectivePipelineDataSource** allows you to manage pipelines from your UI just like any other data source! Pipelines are defined in **JSON** (simple, declarative format) and can be:

✅ Created, started, stopped from the UI
✅ Stored as connection metadata
✅ Shared and version controlled as JSON files
✅ No Python coding required

## How It Works

```
┌─────────────────────────────────────────────┐
│  UI - Create Connection                     │
│  Type: PIPELINE                             │
│  Configuration: JSON                        │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  SubjectivePipelineDataSource               │
│  - Loads JSON configuration                 │
│  - Builds internal pipeline                 │
│  - Manages data source lifecycle            │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  Running Pipeline                           │
│  Node 1 → Node 2 → Node 3                   │
└─────────────────────────────────────────────┘
```

## Quick Start

### 1. Create JSON Configuration

Create a file `my_pipeline.json`:

```json
{
  "name": "VideoTranscription",
  "nodes": [
    {
      "node_id": "monitor",
      "module": "SubjectiveLocalFolderMonitorDataSource",
      "class": "SubjectiveLocalFolderMonitorDataSource",
      "params": {
        "url": "C:\\videos"
      }
    },
    {
      "node_id": "transcriber",
      "module": "SubjectiveTranscribeLocalVideoDataSource",
      "class": "SubjectiveTranscribeLocalVideoDataSource",
      "params": {
        "context_dir": "C:\\transcriptions"
      },
      "dependencies": ["monitor"],
      "filter": "data.get('event_type') == 'created'"
    }
  ]
}
```

### 2. Use in Your UI

When creating a new data source connection in your UI:

**Connection Type:** `PIPELINE`

**Fields:**
- **Pipeline Name:** `VideoTranscription`
- **Pipeline JSON:** _(paste JSON here)_ OR
- **Pipeline JSON File Path:** `path/to/my_pipeline.json`

### 3. Start the Pipeline

In your UI, simply start the data source as you would any other. The pipeline will:
1. Load the JSON configuration
2. Instantiate all data sources
3. Wire up dependencies
4. Start monitoring/processing

## JSON Format

### Minimal Example

```json
{
  "name": "MyPipeline",
  "nodes": [
    {
      "node_id": "source",
      "module": "MySourceModule",
      "class": "MySourceClass",
      "params": {}
    },
    {
      "node_id": "processor",
      "module": "MyProcessorModule",
      "class": "MyProcessorClass",
      "params": {},
      "dependencies": ["source"]
    }
  ]
}
```

### Complete Example with Filters

```json
{
  "name": "VideoTranscriptionPipeline",
  "description": "Monitor folder and transcribe new videos",
  "nodes": [
    {
      "node_id": "folder_monitor",
      "module": "SubjectiveLocalFolderMonitorDataSource",
      "class": "SubjectiveLocalFolderMonitorDataSource",
      "params": {
        "url": "C:\\recordings",
        "connection_name": "RecordingsMonitor"
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
      "filter": "data.get('event_type') == 'created' and str(data.get('path', '')).endswith(('.mp4', '.mkv'))",
      "transform": "data"
    }
  ]
}
```

## Field Reference

### Pipeline Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Pipeline name |
| `description` | No | Human-readable description |
| `nodes` | Yes | Array of node definitions |

### Node Fields

| Field | Required | Description | Example |
|-------|----------|-------------|---------|
| `node_id` | Yes | Unique identifier | `"monitor"` |
| `module` | Yes | Python module path | `"SubjectiveLocalFolderMonitorDataSource"` |
| `class` | Yes | Class name | `"SubjectiveLocalFolderMonitorDataSource"` |
| `params` | No | Constructor parameters | `{"url": "C:\\videos"}` |
| `dependencies` | No | Array of node IDs | `["monitor"]` |
| `filter` | No | Python expression (returns bool) | `"data.get('event_type') == 'created'"` |
| `transform` | No | Python expression (returns data) | `"data.get('path')"` |

## Filter Examples

Only process file creation events:
```json
"filter": "data.get('event_type') == 'created'"
```

Only process video files:
```json
"filter": "str(data.get('path', '')).endswith(('.mp4', '.mkv'))"
```

Combine multiple conditions:
```json
"filter": "data.get('event_type') == 'created' and str(data.get('path', '')).endswith('.mp4')"
```

## UI Integration

### Connection Metadata Schema

Your UI should present these fields when creating a PIPELINE connection:

```javascript
{
  "connection_type": "PIPELINE",
  "fields": [
    {
      "name": "pipeline_name",
      "type": "text",
      "label": "Pipeline Name",
      "required": true
    },
    {
      "name": "pipeline_json",
      "type": "textarea",
      "label": "Pipeline Definition (JSON)",
      "required": false,
      "placeholder": "Paste JSON configuration here"
    },
    {
      "name": "pipeline_json_path",
      "type": "text",
      "label": "Pipeline JSON File Path (Alternative)",
      "required": false,
      "placeholder": "C:\\path\\to\\pipeline.json"
    }
  ]
}
```

### UI Workflow

1. User clicks "Create New Connection"
2. User selects "PIPELINE" type
3. User enters:
   - Pipeline name
   - JSON configuration (inline or file path)
4. User saves connection
5. User starts the data source
6. Pipeline runs in background
7. User can stop/restart like any data source

## Visual Editor Integration

Your visual editor (shown in the screenshot) can:

1. **Load Pipeline JSON** → Display nodes visually
2. **Edit Visually** → Generate JSON automatically
3. **Save** → Store updated JSON in connection metadata

### Editor Features to Support

- ✅ Drag-and-drop nodes
- ✅ Connect nodes visually (dependencies)
- ✅ Edit node parameters in properties panel
- ✅ Add filter/transform expressions
- ✅ Validate pipeline (detect circular dependencies)
- ✅ Export to JSON
- ✅ Import from JSON

## Benefits

| Traditional Approach | Pipeline as Data Source |
|---------------------|------------------------|
| Write Python code | Write JSON config |
| Deploy code changes | Update connection metadata |
| Restart application | Restart data source |
| Version in Git | Version in database |
| Developer task | Admin/user task |

## Examples

### Example 1: Video Transcription
**Use Case:** Automatically transcribe videos added to a folder

```json
{
  "name": "AutoTranscribe",
  "nodes": [
    {
      "node_id": "monitor",
      "module": "SubjectiveLocalFolderMonitorDataSource",
      "class": "SubjectiveLocalFolderMonitorDataSource",
      "params": {"url": "C:\\videos"}
    },
    {
      "node_id": "transcriber",
      "module": "SubjectiveTranscribeLocalVideoDataSource",
      "class": "SubjectiveTranscribeLocalVideoDataSource",
      "params": {"context_dir": "C:\\output"},
      "dependencies": ["monitor"],
      "filter": "data.get('event_type') == 'created'"
    }
  ]
}
```

### Example 2: Three-Stage Pipeline
**Use Case:** Monitor → Filter → Process → Store

```json
{
  "name": "ThreeStage",
  "nodes": [
    {
      "node_id": "monitor",
      "module": "MonitorDataSource",
      "class": "MonitorDataSource",
      "params": {"path": "C:\\input"}
    },
    {
      "node_id": "processor",
      "module": "ProcessorDataSource",
      "class": "ProcessorDataSource",
      "params": {"mode": "batch"},
      "dependencies": ["monitor"],
      "filter": "data.get('size') > 1000"
    },
    {
      "node_id": "storage",
      "module": "StorageDataSource",
      "class": "StorageDataSource",
      "params": {"output": "C:\\output"},
      "dependencies": ["processor"]
    }
  ]
}
```

## Testing

Test your JSON pipeline:

```python
from subjective_abstract_data_source_package import SubjectivePipelineDataSource

pipeline = SubjectivePipelineDataSource(
    params={
        "pipeline_name": "Test",
        "pipeline_json_path": "my_pipeline.json"
    }
)

pipeline.fetch()  # Starts the pipeline
```

## Documentation

- **JSON Schema:** See `PIPELINE_JSON_SCHEMA.md`
- **Examples:** See `examples/` directory
- **Pipeline Guide:** See `PIPELINE_GUIDE.md`

## Summary

**SubjectivePipelineDataSource** turns pipelines into manageable data sources:

✅ **Declarative** - Define in JSON, not Python
✅ **UI-Managed** - Create, start, stop from UI
✅ **Version-Controlled** - JSON files in Git
✅ **User-Friendly** - No coding required
✅ **Visual-Editor-Ready** - Works with your editor

This makes pipelines a **first-class citizen** in your data source ecosystem!
