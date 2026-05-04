"""
Test JSON Pipeline

This example shows how to run a pipeline defined in JSON.
"""

import sys
import os

# Add data source packages to path
sys.path.insert(0, r"C:\brainboost\Subjective\com_subjective_userdata\com_subjective_plugins\subjective_localfoldermonitor_datasource")
sys.path.insert(0, r"C:\brainboost\Subjective\com_subjective_userdata\com_subjective_plugins\subjective_transcribelocalvideo_datasource")

from subjective_abstract_data_source_package import SubjectivePipelineDataSource

# Method 1: Load from JSON file
def test_from_file():
    print("=" * 70)
    print("Testing Pipeline from JSON File")
    print("=" * 70)

    pipeline_ds = SubjectivePipelineDataSource(
        name="TestPipeline",
        params={
            "pipeline_name": "VideoTranscription",
            "pipeline_json_path": "simple_pipeline.json"
        }
    )

    print("Starting pipeline from JSON file...")
    pipeline_ds.fetch()


# Method 2: Load from inline JSON string
def test_from_inline():
    print("=" * 70)
    print("Testing Pipeline from Inline JSON")
    print("=" * 70)

    pipeline_json = '''
    {
      "name": "InlineVideoTranscription",
      "nodes": [
        {
          "node_id": "monitor",
          "module": "SubjectiveLocalFolderMonitorDataSource",
          "class": "SubjectiveLocalFolderMonitorDataSource",
          "params": {
            "url": "C:\\\\videos",
            "connection_name": "VideoMonitor"
          }
        },
        {
          "node_id": "transcriber",
          "module": "SubjectiveTranscribeLocalVideoDataSource",
          "class": "SubjectiveTranscribeLocalVideoDataSource",
          "params": {
            "context_dir": "C:\\\\transcriptions",
            "whisper_model_size": "base"
          },
          "dependencies": ["monitor"],
          "filter": "data.get('event_type') == 'created' and str(data.get('path', '')).endswith(('.mp4', '.mkv'))"
        }
      ]
    }
    '''

    pipeline_ds = SubjectivePipelineDataSource(
        name="InlinePipeline",
        params={
            "pipeline_name": "InlineVideoTranscription",
            "pipeline_json": pipeline_json
        }
    )

    print("Starting pipeline from inline JSON...")
    pipeline_ds.fetch()


if __name__ == "__main__":
    # Change to examples directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    # Choose which test to run
    print("\nSelect test method:")
    print("1. Load from JSON file (simple_pipeline.json)")
    print("2. Load from inline JSON string")

    choice = input("\nEnter choice (1 or 2): ").strip()

    if choice == "1":
        test_from_file()
    elif choice == "2":
        test_from_inline()
    else:
        print("Invalid choice")
