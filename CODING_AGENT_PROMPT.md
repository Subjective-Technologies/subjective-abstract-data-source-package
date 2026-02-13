# Coding Agent Task: Update Pipeline Editor JSON Format

## Context
We have a visual pipeline editor that currently generates an incompatible JSON format. We need to update it to generate JSON that works with our SubjectivePipelineDataSource execution engine while preserving visual layout information.

## Current Problem
**Editor generates this:**
```json
{
  "name": "video_transcribe_pipeline",
  "workbench": {
    "items": [...],
    "connections": [{"from_item": 1, "to_item": 0}]
  }
}
```

**Pipeline runner needs this:**
```json
{
  "name": "VideoTranscriptionPipeline",
  "description": "...",
  "nodes": [
    {
      "node_id": "monitor",
      "module": "SubjectiveLocalFolderMonitorDataSource",
      "class": "SubjectiveLocalFolderMonitorDataSource",
      "params": {"url": "C:\\videos"},
      "dependencies": []
    },
    {
      "node_id": "transcriber",
      "module": "SubjectiveTranscribeLocalVideoDataSource",
      "class": "SubjectiveTranscribeLocalVideoDataSource",
      "params": {"context_dir": "C:\\transcriptions"},
      "dependencies": ["monitor"]
    }
  ]
}
```

## Solution: Hybrid Format
Generate BOTH formats in one file - `nodes` for execution, `workbench` for editor:

```json
{
  "name": "VideoTranscriptionPipeline",
  "description": "Automatically transcribe video files",
  "nodes": [
    {
      "node_id": "local_videos_folder",
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
      "dependencies": ["local_videos_folder"],
      "filter": "data.get('event_type') == 'created'",
      "transform": "data"
    }
  ],
  "workbench": {
    "saved_at": "2026-01-09T16:05:27",
    "nodes": [
      {
        "node_id": "local_videos_folder",
        "position": {"x": 400, "y": 772},
        "display_name": "Video Monitor"
      },
      {
        "node_id": "video_transcriber",
        "position": {"x": 529, "y": 459},
        "display_name": "Transcriber"
      }
    ],
    "connections": [
      {
        "from_node": "video_transcriber",
        "to_node": "local_videos_folder",
        "from_point": "bottom",
        "to_point": "top"
      }
    ]
  }
}
```

## Your Tasks

### 1. Update Save Pipeline Function
Modify the editor's save function to generate the hybrid format:

**Key transformations:**
- `connection_name` → `node_id`
- `ds_class_name` → `module` and `class`
- `internal_data` → `params` (remove: type, ip, ds_class, server, is_local_folder)
- Connections array indices → `dependencies` array of node_ids
- Preserve positions in `workbench.nodes`
- Convert connection indices to node_ids in `workbench.connections`

**Add user input for:**
- Pipeline description (text input when saving)
- Optional filter expression per node (dialog when editing node)
- Optional transform expression per node (dialog when editing node)

### 2. Update Load Pipeline Function
Modify the editor's load function to read the new format:

**Primary source:** Load from `nodes` array
**Visual data:** Use `workbench` if present, otherwise auto-layout
**Reconstruction:**
- Create editor items from nodes
- Calculate connections from dependencies arrays
- Apply positions from workbench.nodes
- Handle both new and legacy formats gracefully

### 3. Add Node Configuration UI
Add to the node edit dialog:

```
┌─────────────────────────────────────────┐
│ Node Configuration                      │
├─────────────────────────────────────────┤
│ Display Name: [Video Monitor         ] │
│                                         │
│ Parameters:                             │
│ [... existing param fields ...]        │
│                                         │
│ Filter Expression (optional):           │
│ ┌─────────────────────────────────────┐ │
│ │ data.get('event_type') == 'created'││ │
│ └─────────────────────────────────────┘ │
│ Python expression returning True/False  │
│                                         │
│ Transform Expression (optional):        │
│ ┌─────────────────────────────────────┐ │
│ │ data                                ││ │
│ └─────────────────────────────────────┘ │
│ Python expression to transform data     │
│                                         │
│ [Cancel]                        [Save]  │
└─────────────────────────────────────────┘
```

## Mapping Reference

### From Editor Item to Pipeline Node
```javascript
// Input: Editor item
{
  connection_name: "monitor",
  ds_class_name: "SubjectiveLocalFolderMonitorDataSource",
  internal_data: {
    url: "C:\\videos",
    connection_name: "monitor",
    type: "Local Folder Monitor",  // REMOVE
    ip: "192.168.0.250",           // REMOVE
    ds_class: "...",               // REMOVE
    server: "192.168.0.250",       // REMOVE
    is_local_folder: false         // REMOVE
  }
}

// Output: Pipeline node
{
  node_id: "monitor",
  module: "SubjectiveLocalFolderMonitorDataSource",
  class: "SubjectiveLocalFolderMonitorDataSource",
  params: {
    url: "C:\\videos",
    connection_name: "monitor"
  },
  dependencies: []  // Calculate from connections
}
```

### From Connections to Dependencies
```javascript
// Input: Editor connections
items = [
  {connection_name: "monitor"},     // index 0
  {connection_name: "transcriber"}  // index 1
]
connections = [
  {from_item: 1, to_item: 0}  // transcriber depends on monitor
]

// Output: Node dependencies
nodes = [
  {node_id: "monitor", dependencies: []},
  {node_id: "transcriber", dependencies: ["monitor"]}
]
```

## Validation Requirements

Before saving, validate:
1. All node_ids are unique
2. No circular dependencies
3. All dependencies reference existing nodes
4. All required params are present

## Testing Checklist

- [ ] Create 2-node pipeline in editor
- [ ] Save to JSON
- [ ] Verify JSON has both `nodes` and `workbench`
- [ ] Verify `nodes` format matches spec
- [ ] Close and reload pipeline
- [ ] Verify visual layout preserved
- [ ] Verify connections correct
- [ ] Test with examples/video_transcription_pipeline.json
- [ ] Load manually created pipeline (nodes only, no workbench)
- [ ] Verify auto-layout works
- [ ] Save and verify workbench added

## File Locations

- **Editor code:** Find the save/load pipeline functions in your pipeline editor codebase
- **Test with:** `examples/video_transcription_pipeline.json`
- **Spec details:** See `PIPELINE_EDITOR_FORMAT_SPEC.md` for complete specification

## Example Code (Pseudocode)

```javascript
function savePipeline() {
  // Build nodes array from editor items
  const nodes = items.map((item, idx) => ({
    node_id: item.connection_name,
    module: item.ds_class_name,
    class: item.ds_class_name,
    params: cleanParams(item.internal_data),
    dependencies: getDependencies(idx, connections, items),
    ...(item.filter && {filter: item.filter}),
    ...(item.transform && {transform: item.transform})
  }));

  // Build workbench metadata
  const workbench = {
    saved_at: new Date().toISOString(),
    nodes: items.map(i => ({
      node_id: i.connection_name,
      position: i.position,
      display_name: i.display_name
    })),
    connections: connections.map(c => ({
      from_node: items[c.from_item].connection_name,
      to_node: items[c.to_item].connection_name,
      from_point: c.from_point,
      to_point: c.to_point
    }))
  };

  return {
    name: pipelineName,
    description: pipelineDescription,
    nodes: nodes,
    workbench: workbench
  };
}

function cleanParams(internal_data) {
  const params = {...internal_data};
  delete params.type;
  delete params.ip;
  delete params.ds_class;
  delete params.server;
  delete params.is_local_folder;
  return params;
}

function getDependencies(fromIdx, connections, items) {
  return connections
    .filter(c => c.from_item === fromIdx)
    .map(c => items[c.to_item].connection_name);
}
```

## Questions?
Refer to `PIPELINE_EDITOR_FORMAT_SPEC.md` for complete details on:
- Field descriptions
- Validation rules
- Load pipeline implementation
- Edge cases and error handling
