"""
Simple Pipeline Example

This is the simplest possible example showing how to create a pipeline
that connects two data sources.
"""

import sys
sys.path.insert(0, r"C:\brainboost\Subjective\com_subjective_userdata\com_subjective_plugins\subjective_localfoldermonitor_datasource")
sys.path.insert(0, r"C:\brainboost\Subjective\com_subjective_userdata\com_subjective_plugins\subjective_transcribelocalvideo_datasource")

from subjective_abstract_data_source_package import SubjectiveDataSourcePipeline
from SubjectiveLocalFolderMonitorDataSource import SubjectiveLocalFolderMonitorDataSource
from SubjectiveTranscribeLocalVideoDataSource import SubjectiveTranscribeLocalVideoDataSource
import time


# Simple filter: only process video file creation events
def only_new_videos(data):
    """Filter to only process new video files"""
    return (isinstance(data, dict) and
            data.get('event_type') == 'created' and
            str(data.get('path', '')).endswith(('.mp4', '.mkv')))


# Create and configure the pipeline
pipeline = SubjectiveDataSourcePipeline(name="SimpleVideoTranscription")

# Step 1: Add the folder monitor (watches for file changes)
pipeline.add_node(
    node_id="folder_monitor",
    data_source_class=SubjectiveLocalFolderMonitorDataSource,
    params={
        "url": r"C:\brainboost\videos",
        "connection_name": "VideoMonitor"
    }
)

# Step 2: Add the transcriber (processes videos from monitor)
pipeline.add_node(
    node_id="transcriber",
    data_source_class=SubjectiveTranscribeLocalVideoDataSource,
    params={
        "context_dir": r"C:\brainboost\transcriptions",
        "whisper_model_size": "base"
    },
    dependencies=["folder_monitor"],  # Transcriber depends on folder_monitor
    filter_fn=only_new_videos  # Only process new video files
)

# Step 3: Start the pipeline
print("Starting video transcription pipeline...")
print("Add .mp4 or .mkv files to C:\\brainboost\\videos to transcribe them")
print("Press Ctrl+C to stop\n")

pipeline.build()
pipeline.start()

# Keep running until user stops
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopping pipeline...")
    pipeline.stop()
    print("Done!")
