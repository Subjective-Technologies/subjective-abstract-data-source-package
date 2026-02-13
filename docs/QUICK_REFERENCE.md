# Pipeline Quick Reference

## 🚀 Quickest Way to Get Started

### Step 1: Create JSON file
```json
{
  "name": "MyPipeline",
  "nodes": [
    {
      "node_id": "source",
      "module": "MySourceModule",
      "class": "MySourceClass",
      "params": {"key": "value"}
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

### Step 2: Load in Python
```python
from subjective_abstract_data_source_package import SubjectivePipelineDataSource

pipeline = SubjectivePipelineDataSource(
    params={
        "pipeline_name": "MyPipeline",
        "pipeline_json_path": "my_pipeline.json"
    }
)

pipeline.fetch()
```

## 📋 JSON Template

```json
{
  "name": "PipelineName",
  "description": "What this pipeline does",
  "nodes": [
    {
      "node_id": "unique_id",
      "module": "python.module.path",
      "class": "ClassName",
      "params": {},
      "dependencies": [],
      "filter": "optional_expression",
      "transform": "optional_expression"
    }
  ]
}
```

## 🔍 Common Filters

Only new files:
```json
"filter": "data.get('event_type') == 'created'"
```

Only video files:
```json
"filter": "str(data.get('path', '')).endswith(('.mp4', '.mkv'))"
```

Combined:
```json
"filter": "data.get('event_type') == 'created' and str(data.get('path', '')).endswith('.mp4')"
```

## 🎯 Video Transcription Example

```json
{
  "name": "AutoTranscribe",
  "nodes": [
    {
      "node_id": "monitor",
      "module": "SubjectiveLocalFolderMonitorDataSource",
      "class": "SubjectiveLocalFolderMonitorDataSource",
      "params": {
        "url": "C:\\videos",
        "connection_name": "Monitor"
      }
    },
    {
      "node_id": "transcriber",
      "module": "SubjectiveTranscribeLocalVideoDataSource",
      "class": "SubjectiveTranscribeLocalVideoDataSource",
      "params": {
        "context_dir": "C:\\output",
        "whisper_model_size": "base"
      },
      "dependencies": ["monitor"],
      "filter": "data.get('event_type') == 'created' and str(data.get('path', '')).endswith(('.mp4', '.mkv'))"
    }
  ]
}
```

## 🎨 UI Integration

Connection fields to present:

| Field | Type | Required |
|-------|------|----------|
| pipeline_name | text | Yes |
| pipeline_json | textarea | No* |
| pipeline_json_path | text | No* |

*One of the JSON fields is required

## 🐍 Python API

```python
from subjective_abstract_data_source_package import SubjectiveDataSourcePipeline

pipeline = SubjectiveDataSourcePipeline(name="MyPipeline")

pipeline.add_node(
    node_id="id",
    data_source_class=MyClass,
    params={},
    dependencies=["other_id"],
    filter_fn=lambda data: True,
    transform_fn=lambda data: data
)

pipeline.start()
```

## ✅ Validation Checklist

- [ ] Valid JSON syntax
- [ ] All nodes have unique node_id
- [ ] All nodes have module and class
- [ ] Dependencies reference existing nodes
- [ ] No circular dependencies
- [ ] Filter expressions are valid Python

## 🔧 Troubleshooting

| Problem | Solution |
|---------|----------|
| "Invalid JSON" | Check commas, brackets, quotes |
| "Module not found" | Verify module path and installation |
| "Circular dependency" | Check dependency graph |
| "No pipeline_json" | Provide either pipeline_json or pipeline_json_path |

## 📚 Documentation

| File | Purpose |
|------|---------|
| PIPELINE_README.md | Getting started |
| PIPELINE_JSON_SCHEMA.md | JSON format spec |
| PIPELINE_AS_DATASOURCE.md | UI integration |
| PIPELINE_GUIDE.md | Complete guide |

## 🎯 Example Files

- `examples/simple_pipeline.json`
- `examples/video_transcription_pipeline.json`
- `examples/test_json_pipeline.py`

## 💡 Tips

1. **Start simple** - Begin with 2 nodes, add more later
2. **Use absolute paths** - Avoid relative paths in params
3. **Escape backslashes** - Windows: `"C:\\path"` not `"C:\path"`
4. **Test filters** - Verify filter expressions work as expected
5. **Check logs** - BBLogger shows pipeline execution details

## 🚦 Quick Start Checklist

- [ ] Install required data sources
- [ ] Create JSON configuration
- [ ] Test JSON validity (use JSON validator)
- [ ] Create SubjectivePipelineDataSource instance
- [ ] Call fetch() to start
- [ ] Monitor logs for issues
- [ ] Add to UI as PIPELINE connection type
