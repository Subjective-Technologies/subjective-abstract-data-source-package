# Pipeline Editor JSON Format Specification

## Objective
Update the pipeline editor to generate JSON files that are compatible with the SubjectivePipelineDataSource execution format while preserving visual layout information for the editor.

## Current Editor Output Format (Incompatible)
```json
{
  "name": "video_transcribe_pipeline",
  "saved_at": "2026-01-09T16:05:27",
  "workbench": {
    "items": [
      {
        "connection_name": "local_videos_monitor",
        "display_name": "local_videos_monitor",
        "ds_type": "Local Folder Monitor",
        "ds_class_name": "SubjectiveLocalFolderMonitorDataSource",
        "position": {"x": 400, "y": 772},
        "internal_data": {
          "type": "Local Folder Monitor",
          "ip": "192.168.0.250",
          "ds_class": "SubjectiveLocalFolderMonitorDataSource",
          "connection_name": "local_videos_monitor",
          "is_local_folder": false,
          "url": "C:\\Users\\pablo\\Videos",
          "server": "192.168.0.250"
        },
        "is_local_folder": false
      }
    ],
    "connections": [
      {
        "from_item": 1,
        "to_item": 0,
        "from_point": "bottom",
        "to_point": "top"
      }
    ]
  }
}
```

## Required Pipeline Execution Format
```json
{
  "name": "VideoTranscriptionPipeline",
  "description": "Automatically transcribe video files when added to a monitored folder",
  "nodes": [
    {
      "node_id": "local_videos_folder",
      "module": "SubjectiveLocalFolderMonitorDataSource",
      "class": "SubjectiveLocalFolderMonitorDataSource",
      "params": {
        "url": "C:\\brainboost\\videos",
        "connection_name": "VideoFolderMonitor"
      },
      "dependencies": []
    },
    {
      "node_id": "video_transcriber",
      "module": "SubjectiveTranscribeLocalVideoDataSource",
      "class": "SubjectiveTranscribeLocalVideoDataSource",
      "params": {
        "context_dir": "C:\\brainboost\\transcriptions",
        "whisper_model_size": "base"
      },
      "dependencies": ["local_videos_folder"],
      "filter": "data.get('event_type') == 'created' and str(data.get('path', '')).endswith(('.mp4', '.mkv'))",
      "transform": "data"
    }
  ]
}
```

## Target Hybrid Format (Pipeline-Compatible + Editor Metadata)

The editor should generate files that combine both formats:

```json
{
  "name": "VideoTranscriptionPipeline",
  "description": "Automatically transcribe video files when added to a monitored folder",
  "nodes": [
    {
      "node_id": "local_videos_folder",
      "module": "SubjectiveLocalFolderMonitorDataSource",
      "class": "SubjectiveLocalFolderMonitorDataSource",
      "params": {
        "url": "C:\\brainboost\\videos",
        "connection_name": "VideoFolderMonitor"
      },
      "dependencies": []
    },
    {
      "node_id": "video_transcriber",
      "module": "SubjectiveTranscribeLocalVideoDataSource",
      "class": "SubjectiveTranscribeLocalVideoDataSource",
      "params": {
        "context_dir": "C:\\brainboost\\transcriptions",
        "whisper_model_size": "base"
      },
      "dependencies": ["local_videos_folder"],
      "filter": "data.get('event_type') == 'created' and str(data.get('path', '')).endswith(('.mp4', '.mkv'))",
      "transform": "data"
    }
  ],
  "workbench": {
    "saved_at": "2026-01-09T16:05:27",
    "nodes": [
      {
        "node_id": "local_videos_folder",
        "position": {"x": 400, "y": 772},
        "display_name": "local_videos_monitor"
      },
      {
        "node_id": "video_transcriber",
        "position": {"x": 529, "y": 459},
        "display_name": "video_transcriber"
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

## Format Requirements

### Main Structure
1. **name** (string, required): Pipeline name
2. **description** (string, optional): Pipeline description
3. **nodes** (array, required): Execution nodes in pipeline format
4. **workbench** (object, optional): Visual editor metadata (ignored by pipeline runner)

### Node Structure (Required for Execution)
Each node in the `nodes` array must have:

- **node_id** (string, required): Unique identifier for this node
  - Use the connection_name or generate a unique ID
  - Must be valid Python identifier (no spaces, alphanumeric + underscore)

- **module** (string, required): Python module name containing the data source class
  - Should be the ds_class_name from editor (e.g., "SubjectiveLocalFolderMonitorDataSource")

- **class** (string, required): Data source class name
  - Same as module for most cases

- **params** (object, required): Parameters passed to the data source constructor
  - Extract from internal_data, removing metadata fields like:
    - "type", "ip", "ds_class", "server", "is_local_folder"
  - Keep only actual configuration parameters like:
    - "url", "connection_name", "videos_dir", "context_dir", "whisper_model_size", etc.

- **dependencies** (array, required): Array of node_id strings this node depends on
  - Derived from editor connections
  - Empty array [] if this is a root node (no dependencies)

- **filter** (string, optional): Python expression to filter data before passing to this node
  - Evaluate to True/False
  - Has access to `data` variable containing the input
  - Example: `"data.get('event_type') == 'created' and str(data.get('path', '')).endswith(('.mp4', '.mkv'))"`

- **transform** (string, optional): Python expression to transform data before passing to node
  - Should return the transformed data
  - Example: `"data"` (pass through as-is)
  - Example: `"{'video_path': data.get('path'), 'timestamp': data.get('timestamp')}"`

### Workbench Structure (Optional, for Editor)
The `workbench` object contains visual metadata:

- **saved_at** (string): ISO timestamp when saved
- **nodes** (array): Visual node information
  - **node_id** (string): References the node_id in main nodes array
  - **position** (object): {x: number, y: number} canvas coordinates
  - **display_name** (string): Human-readable name shown in editor
- **connections** (array): Visual connection information
  - **from_node** (string): Source node_id
  - **to_node** (string): Target node_id (dependency)
  - **from_point** (string): Connection anchor point ("top", "bottom", "left", "right")
  - **to_point** (string): Connection anchor point ("top", "bottom", "left", "right")

## Mapping Guide: Editor Data → Pipeline Format

### Item to Node Mapping
```javascript
// Editor item
{
  "connection_name": "local_videos_monitor",
  "ds_class_name": "SubjectiveLocalFolderMonitorDataSource",
  "internal_data": {
    "url": "C:\\Users\\pablo\\Videos",
    "connection_name": "local_videos_monitor",
    // ... other params
  }
}

// Maps to pipeline node
{
  "node_id": "local_videos_monitor",  // from connection_name
  "module": "SubjectiveLocalFolderMonitorDataSource",  // from ds_class_name
  "class": "SubjectiveLocalFolderMonitorDataSource",   // from ds_class_name
  "params": {
    "url": "C:\\Users\\pablo\\Videos",
    "connection_name": "local_videos_monitor"
    // Extract only actual config params from internal_data
  },
  "dependencies": []  // Calculate from connections
}
```

### Connection to Dependencies Mapping
```javascript
// Editor connections (array index based)
{
  "from_item": 1,  // video_transcriber (index in items array)
  "to_item": 0,    // local_videos_monitor (index in items array)
}

// Maps to dependencies in node
{
  "node_id": "video_transcriber",
  "dependencies": ["local_videos_monitor"]  // to_item becomes dependency
}
```

## Implementation Tasks

### Task 1: Update Save Pipeline Function
Modify the save pipeline function in the editor to:

1. **Generate nodes array**: Transform items into proper node format
   - Extract node_id from connection_name
   - Set module and class from ds_class_name
   - Clean params by removing metadata fields
   - Calculate dependencies from connections array

2. **Calculate dependencies**: For each node
   - Find all connections where this node is `from_item`
   - Add corresponding `to_item` node_ids to dependencies array
   - Maintain dependency order (topological sort if needed)

3. **Add workbench metadata**: Preserve visual layout
   - Map items array to workbench.nodes with positions
   - Convert connection indices to node_ids in workbench.connections

4. **Add description field**: Prompt user or generate from pipeline name

### Task 2: Update Load Pipeline Function
Modify the load pipeline function to:

1. **Primary: Load from nodes array** (execution format)
   - Create editor items from nodes
   - Reconstruct internal_data from params + metadata
   - Use workbench.nodes for positions if available
   - Fall back to auto-layout if workbench missing

2. **Use workbench for visual data** (if present)
   - Apply saved positions from workbench.nodes
   - Reconstruct connections from dependencies + workbench.connections visual info

3. **Handle legacy format**: If file has workbench but no nodes array
   - Convert old format to new format
   - Warn user to re-save

### Task 3: Add Filter/Transform UI
Add UI elements to node editor dialog:

1. **Filter Expression Input** (optional text area)
   - Label: "Filter Expression (optional)"
   - Placeholder: "data.get('event_type') == 'created'"
   - Help text: "Python expression returning True/False to filter incoming data"

2. **Transform Expression Input** (optional text area)
   - Label: "Transform Expression (optional)"
   - Placeholder: "data"
   - Help text: "Python expression to transform data before passing to this node"

3. **Store in node data**: Save filter/transform when user edits node

## Validation Rules

1. **node_id uniqueness**: All node_ids must be unique within a pipeline
2. **dependency validity**: All node_ids in dependencies array must exist
3. **no circular dependencies**: Detect and prevent circular dependency chains
4. **params required**: Each node must have a params object (can be empty {})
5. **workbench-nodes sync**: If workbench exists, all nodes must have corresponding workbench.nodes entries

## Testing Requirements

### Test Case 1: Save and Reload
1. Create a pipeline in editor with 2+ nodes
2. Save to JSON
3. Verify JSON has both `nodes` and `workbench` sections
4. Verify `nodes` format matches specification
5. Close editor and reload pipeline
6. Verify visual layout is preserved
7. Verify connections are correct

### Test Case 2: Pipeline Execution Compatibility
1. Create pipeline in editor
2. Save to JSON
3. Load in SubjectivePipelineDataSource
4. Verify pipeline executes correctly
5. Verify workbench section is ignored by pipeline runner

### Test Case 3: Manual JSON Compatibility
1. Create pipeline JSON manually (nodes only, no workbench)
2. Load in editor
3. Verify nodes appear correctly (with auto-layout)
4. Save from editor
5. Verify workbench section is added
6. Verify nodes section remains unchanged

## Example Implementation Pseudocode

```javascript
// Save Pipeline
function savePipeline() {
  const nodes = workbenchItems.map((item, index) => {
    // Calculate dependencies from connections
    const dependencies = connections
      .filter(conn => conn.from_item === index)
      .map(conn => workbenchItems[conn.to_item].connection_name);

    // Extract clean params (remove metadata)
    const params = { ...item.internal_data };
    delete params.type;
    delete params.ip;
    delete params.ds_class;
    delete params.server;
    delete params.is_local_folder;

    return {
      node_id: item.connection_name,
      module: item.ds_class_name,
      class: item.ds_class_name,
      params: params,
      dependencies: dependencies,
      // Add filter/transform if set
      ...(item.filter && { filter: item.filter }),
      ...(item.transform && { transform: item.transform })
    };
  });

  const workbench = {
    saved_at: new Date().toISOString(),
    nodes: workbenchItems.map(item => ({
      node_id: item.connection_name,
      position: item.position,
      display_name: item.display_name
    })),
    connections: connections.map(conn => ({
      from_node: workbenchItems[conn.from_item].connection_name,
      to_node: workbenchItems[conn.to_item].connection_name,
      from_point: conn.from_point,
      to_point: conn.to_point
    }))
  };

  return {
    name: pipelineName,
    description: pipelineDescription,
    nodes: nodes,
    workbench: workbench
  };
}

// Load Pipeline
function loadPipeline(json) {
  if (!json.nodes) {
    // Legacy format or invalid
    console.error("Invalid pipeline: missing nodes array");
    return;
  }

  // Load from nodes (execution format)
  const items = json.nodes.map(node => {
    // Find position from workbench if available
    const workbenchNode = json.workbench?.nodes.find(n => n.node_id === node.node_id);

    return {
      connection_name: node.node_id,
      display_name: workbenchNode?.display_name || node.node_id,
      ds_type: extractTypeFromClass(node.class),
      ds_class_name: node.class,
      position: workbenchNode?.position || autoCalculatePosition(),
      internal_data: {
        ...node.params,
        type: extractTypeFromClass(node.class),
        ds_class: node.class,
        connection_name: node.node_id
      },
      filter: node.filter,
      transform: node.transform
    };
  });

  // Reconstruct connections from dependencies
  const connections = [];
  json.nodes.forEach((node, fromIndex) => {
    node.dependencies.forEach(depNodeId => {
      const toIndex = json.nodes.findIndex(n => n.node_id === depNodeId);
      const workbenchConn = json.workbench?.connections.find(
        c => c.from_node === node.node_id && c.to_node === depNodeId
      );

      connections.push({
        from_item: fromIndex,
        to_item: toIndex,
        from_point: workbenchConn?.from_point || "bottom",
        to_point: workbenchConn?.to_point || "top"
      });
    });
  });

  return { items, connections };
}
```

## Notes for Pipeline Runner
The SubjectivePipelineDataSource should:
1. **Only read the `nodes` array** - ignore workbench completely
2. **Validate node structure** before execution
3. **Build dependency graph** from dependencies arrays
4. **Execute in topological order** respecting dependencies
5. **Apply filter expressions** before passing data to nodes
6. **Apply transform expressions** to modify data flow

The workbench section is purely for the visual editor and should have zero impact on pipeline execution.
