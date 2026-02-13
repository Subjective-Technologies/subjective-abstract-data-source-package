# Pipeline System Architecture

## Three Levels of Abstraction

```
┌─────────────────────────────────────────────────────────────────┐
│                          LEVEL 3: UI                            │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐           │
│  │  Connection  │  │   Visual    │  │   Start/    │           │
│  │  Management  │  │   Editor    │  │   Stop      │           │
│  └─────────────┘  └─────────────┘  └─────────────┘           │
│                                                                 │
│  User creates pipeline like any other data source               │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            │ JSON Configuration
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│              LEVEL 2: SubjectivePipelineDataSource              │
│                     (Pipeline as Data Source)                   │
│                                                                 │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  • Loads JSON configuration                            │   │
│  │  • Implements SubjectiveDataSource interface           │   │
│  │  • Manageable from UI like any data source             │   │
│  │  • fetch() → builds and starts pipeline                │   │
│  │  • stop() → stops pipeline                             │   │
│  └────────────────────────────────────────────────────────┘   │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            │ Uses
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│          LEVEL 1: SubjectiveDataSourcePipeline                  │
│                     (Pipeline Engine)                           │
│                                                                 │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  • Manages pipeline nodes                              │   │
│  │  • Validates dependencies (no cycles)                  │   │
│  │  • Starts nodes in correct order                       │   │
│  │  • Coordinates data flow                               │   │
│  │  • Applies filters and transforms                      │   │
│  └────────────────────────────────────────────────────────┘   │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            │ Orchestrates
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                  Individual Data Sources                        │
│                                                                 │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐      │
│  │LocalFolder   │   │ Transcribe   │   │   Other      │      │
│  │Monitor       │→  │ Video        │→  │   Sources    │      │
│  └──────────────┘   └──────────────┘   └──────────────┘      │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow Example: Video Transcription

```
┌─────────────────────────────────────────────────────────────────┐
│                          USER ACTION                            │
│                                                                 │
│  User adds video.mp4 to C:\videos folder                        │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            │ File system event
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                  LocalFolderMonitorDataSource                   │
│                                                                 │
│  • Detects file creation                                        │
│  • Creates notification data:                                   │
│    {                                                            │
│      "event_type": "created",                                   │
│      "path": "C:\\videos\\video.mp4",                           │
│      "timestamp": 1234567890                                    │
│    }                                                            │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            │ send_notification()
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                      PipelineAdapter                            │
│                                                                 │
│  1. Receives notification                                       │
│  2. Applies filter:                                             │
│     ✓ event_type == 'created'                                   │
│     ✓ path ends with .mp4                                       │
│  3. Passes filter → continue                                    │
│  4. Applies transform (if any)                                  │
│  5. Calls process_input() on dependent                          │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            │ process_input(data)
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│              TranscribeLocalVideoDataSource                     │
│                                                                 │
│  • Receives pipeline input                                      │
│  • Extracts video path from data                                │
│  • Loads Whisper model                                          │
│  • Transcribes video                                            │
│  • Saves to C:\transcriptions\context-*.json                    │
│  • Calls update() to notify subscribers                         │
└─────────────────────────────────────────────────────────────────┘
```

## Component Relationships

```
SubjectivePipelineDataSource
    │
    ├─── Owns ──→ SubjectiveDataSourcePipeline
    │                 │
    │                 ├─── Contains ──→ PipelineNode (monitor)
    │                 │                      │
    │                 │                      └─── Has ──→ DataSource Instance
    │                 │
    │                 └─── Contains ──→ PipelineNode (transcriber)
    │                                        │
    │                                        ├─── Has ──→ DataSource Instance
    │                                        └─── Has ──→ PipelineAdapter
    │                                                    │
    │                                                    └─── Subscribes to monitor
    └─── Implements ──→ SubjectiveDataSource Interface
```

## Pipeline Node State Machine

```
    ┌──────────────┐
    │   DEFINED    │  Node added to pipeline
    └──────┬───────┘
           │
           │ pipeline.build()
           ↓
    ┌──────────────┐
    │ INSTANTIATED │  Data source created
    └──────┬───────┘
           │
           │ pipeline.start()
           ↓
    ┌──────────────┐
    │   RUNNING    │  Data source fetch() called
    └──────┬───────┘
           │
           │ pipeline.stop()
           ↓
    ┌──────────────┐
    │   STOPPED    │  Data source stopped
    └──────────────┘
```

## Dependency Graph Example

```
Pipeline: "VideoProcessing"

    ┌──────────────┐
    │   monitor    │  (no dependencies)
    └──────┬───────┘
           │
           │ dependency
           ↓
    ┌──────────────┐
    │ transcriber  │  (depends on: monitor)
    └──────┬───────┘
           │
           │ dependency
           ↓
    ┌──────────────┐
    │  summarizer  │  (depends on: transcriber)
    └──────────────┘

Start order: monitor → transcriber → summarizer
```

## Filter and Transform Flow

```
Dependency sends update
         │
         ↓
    ┌─────────┐
    │ Filter? │──No──→ Drop update
    └────┬────┘
         │Yes
         ↓
    ┌─────────┐
    │Transform│  Modify data
    └────┬────┘
         │
         ↓
    process_input(transformed_data)
```

## JSON to Pipeline Conversion

```
JSON Configuration               Pipeline Object
─────────────────                ───────────────

{                                SubjectiveDataSourcePipeline
  "name": "MyPipeline",  ──────→   name = "MyPipeline"
  "nodes": [                        nodes = {
    {                                 "monitor": PipelineNode(
      "node_id": "monitor", ────────→   node_id = "monitor"
      "module": "...",      ────────→   data_source_class = ...
      "class": "...",                   params = {...}
      "params": {...}                   dependencies = []
    },                                )
    {                                 "proc": PipelineNode(
      "node_id": "proc",   ─────────→   node_id = "proc"
      "dependencies":      ─────────→   dependencies = ["monitor"]
        ["monitor"]                     filter_fn = ...
      "filter": "..."      ─────────→   transform_fn = ...
    }                                 )
  ]                               }
}
```

## UI to Pipeline Flow

```
┌──────────────────────────────────────────────────────────┐
│                      USER ACTIONS                        │
└──────────────────────────────────────────────────────────┘
    │
    │ 1. Create Connection
    │    Type: PIPELINE
    │    Name: VideoTranscription
    │    JSON: {...}
    │
    ↓
┌──────────────────────────────────────────────────────────┐
│                   CONNECTION SAVED                       │
│  connection_metadata = {                                 │
│    "type": "PIPELINE",                                   │
│    "params": {                                           │
│      "pipeline_name": "VideoTranscription",              │
│      "pipeline_json": "{...}"                            │
│    }                                                     │
│  }                                                       │
└──────────────────────────────────────────────────────────┘
    │
    │ 2. User clicks "Start"
    │
    ↓
┌──────────────────────────────────────────────────────────┐
│              DATASOURCE INSTANTIATED                     │
│  ds = SubjectivePipelineDataSource(                      │
│    params=connection_metadata["params"]                  │
│  )                                                       │
└──────────────────────────────────────────────────────────┘
    │
    │ 3. ds.fetch() called
    │
    ↓
┌──────────────────────────────────────────────────────────┐
│                 PIPELINE BUILT & STARTED                 │
│  • Load JSON                                             │
│  • Create SubjectiveDataSourcePipeline                   │
│  • Add nodes from JSON                                   │
│  • Build pipeline                                        │
│  • Start pipeline                                        │
└──────────────────────────────────────────────────────────┘
    │
    │ 4. Pipeline running
    │
    ↓
┌──────────────────────────────────────────────────────────┐
│                   DATA PROCESSING                        │
│  monitor → transcriber → (results)                       │
└──────────────────────────────────────────────────────────┘
```

## Class Diagram

```
┌─────────────────────────────┐
│   SubjectiveDataSource      │
│   (Abstract Base Class)     │
└──────────────┬──────────────┘
               │
               │ inherits
               │
┌──────────────┴──────────────┐
│SubjectivePipelineDataSource │
│                             │
│ + fetch()                   │
│ + stop()                    │
│ + get_icon()                │
│ + get_connection_data()     │
│ - _load_pipeline_config()   │
│ - _build_pipeline_from...() │
└──────────────┬──────────────┘
               │
               │ owns
               ↓
┌──────────────────────────────┐
│SubjectiveDataSourcePipeline  │
│                              │
│ + add_node()                 │
│ + build()                    │
│ + start()                    │
│ + stop()                     │
│ - _validate_dependencies()   │
└──────────────┬───────────────┘
               │
               │ contains
               ↓
┌──────────────────────────────┐
│      PipelineNode            │
│                              │
│ + node_id                    │
│ + data_source_class          │
│ + params                     │
│ + dependencies               │
│ + filter_fn                  │
│ + transform_fn               │
│ + instance                   │
└──────────────────────────────┘
```

## Summary

The pipeline architecture provides three levels:

1. **Engine Level** - Core orchestration logic
2. **Data Source Level** - Pipeline as a manageable data source
3. **UI Level** - Visual management and JSON configuration

This layered approach allows:
- Developers to use Python API
- Users to use JSON configuration
- UI to provide visual editing

All three levels work together seamlessly!
