"""
Example: Video Transcription Pipeline

This example demonstrates how to create a pipeline that:
1. Monitors a folder for new video files
2. Automatically transcribes new videos when they are detected

The pipeline coordinates two data sources:
- LocalFolderMonitor: Watches for file changes in a folder
- TranscribeLocalVideo: Transcribes video files when triggered
"""

import sys
import os
import time

# Add the data source packages to the path
sys.path.insert(0, r"C:\brainboost\Subjective\com_subjective_userdata\com_subjective_plugins\subjective_localfoldermonitor_datasource")
sys.path.insert(0, r"C:\brainboost\Subjective\com_subjective_userdata\com_subjective_plugins\subjective_transcribelocalvideo_datasource")

from subjective_abstract_data_source_package import SubjectiveDataSourcePipeline
from SubjectiveLocalFolderMonitorDataSource import SubjectiveLocalFolderMonitorDataSource
from SubjectiveTranscribeLocalVideoDataSource import SubjectiveTranscribeLocalVideoDataSource


def is_video_file(data):
    """
    Filter function: Only process video file events.

    Args:
        data: Notification data from the folder monitor

    Returns:
        True if this is a video file event, False otherwise
    """
    if not isinstance(data, dict):
        return False

    # Only process 'created' events (not modified, deleted, etc.)
    event_type = data.get('event_type')
    if event_type != 'created':
        return False

    # Check if it's a video file
    file_path = data.get('path', '')
    return file_path.endswith(('.mp4', '.mkv'))


def extract_video_path(data):
    """
    Transform function: Extract just the video file path from the notification.

    Args:
        data: Notification data from the folder monitor

    Returns:
        The transformed data (in this case, just passes through since
        process_input handles the extraction)
    """
    # The TranscribeLocalVideoDataSource.process_input method already handles
    # extracting the path, so we just pass through the data
    return data


def main():
    """
    Main function to set up and run the video transcription pipeline.
    """

    # Configuration
    VIDEOS_FOLDER = r"C:\brainboost\videos"  # Folder to monitor for video files
    CONTEXT_OUTPUT = r"C:\brainboost\context"  # Folder to save transcriptions
    WHISPER_MODEL = "base"  # Whisper model size: tiny, base, small, medium, large

    print("=" * 70)
    print("Video Transcription Pipeline Example")
    print("=" * 70)
    print(f"Monitoring folder: {VIDEOS_FOLDER}")
    print(f"Saving transcriptions to: {CONTEXT_OUTPUT}")
    print(f"Using Whisper model: {WHISPER_MODEL}")
    print("=" * 70)

    # Create the pipeline
    pipeline = SubjectiveDataSourcePipeline(name="VideoTranscriptionPipeline")

    # Add the folder monitor node (source)
    pipeline.add_node(
        node_id="folder_monitor",
        data_source_class=SubjectiveLocalFolderMonitorDataSource,
        params={
            "url": VIDEOS_FOLDER,
            "connection_name": "VideoFolderMonitor"
        },
        dependencies=[]  # No dependencies - this is the source
    )

    # Add the video transcription node (dependent)
    pipeline.add_node(
        node_id="video_transcriber",
        data_source_class=SubjectiveTranscribeLocalVideoDataSource,
        params={
            "context_dir": CONTEXT_OUTPUT,
            "whisper_model_size": WHISPER_MODEL
        },
        dependencies=["folder_monitor"],  # Depends on folder_monitor
        filter_fn=is_video_file,  # Only process video files
        transform_fn=extract_video_path  # Extract video path from notification
    )

    # Build and start the pipeline
    print("\nBuilding pipeline...")
    pipeline.build()

    print("Starting pipeline...")
    pipeline.start()

    print("\nPipeline is running!")
    print("Add .mp4 or .mkv files to the monitored folder to see transcription in action.")
    print("Press Ctrl+C to stop the pipeline.\n")

    try:
        # Keep the pipeline running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nStopping pipeline...")
        pipeline.stop()
        print("Pipeline stopped. Goodbye!")


if __name__ == "__main__":
    main()
