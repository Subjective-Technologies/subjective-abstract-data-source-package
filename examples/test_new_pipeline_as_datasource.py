"""
Example: Using SubjectiveDataSourcePipeline as a Data Source

This demonstrates the new capability where SubjectiveDataSourcePipeline
inherits from SubjectiveDataSource, allowing pipelines to be used like
any other data source.
"""

import sys
import os

# Add data source packages to path
sys.path.insert(0, r"C:\brainboost\Subjective\com_subjective_userdata\com_subjective_plugins\subjective_localfoldermonitor_datasource")
sys.path.insert(0, r"C:\brainboost\Subjective\com_subjective_userdata\com_subjective_plugins\subjective_transcribelocalvideo_datasource")

# Use the NEW implementation
from subjective_abstract_data_source_package.SubjectiveDataSourcePipeline_NEW import SubjectiveDataSourcePipeline


def example_1_from_pipe_file():
    """
    Example 1: Load pipeline from a .pipe file

    This is the simplest usage - just provide a path to a .pipe file.
    """
    print("=" * 70)
    print("Example 1: Loading Pipeline from .pipe File")
    print("=" * 70)

    # Path to your pipeline file
    pipeline_path = r"c:\brainboost\Subjective\com_subjective_userdata\com_subjective_pipelines\video_transcript_pipeline.pipe"

    # Create pipeline data source from file
    my_pipeline_ds = SubjectiveDataSourcePipeline(
        name="VideoTranscriptionPipeline",
        params={
            "pipeline_file": pipeline_path
        }
    )

    print(f"Pipeline loaded from: {pipeline_path}")
    print(f"Pipeline name: {my_pipeline_ds.get_name()}")
    print(f"Number of nodes: {len(my_pipeline_ds.nodes)}")

    # Now fetch() works like any data source
    print("\nStarting pipeline with fetch()...")
    try:
        my_pipeline_ds.fetch()
    except KeyboardInterrupt:
        print("\nStopping pipeline...")
        my_pipeline_ds.stop()


def example_2_programmatic_configuration():
    """
    Example 2: Configure pipeline programmatically

    Build the pipeline by adding nodes in code.
    """
    print("=" * 70)
    print("Example 2: Programmatic Pipeline Configuration")
    print("=" * 70)

    from SubjectiveLocalFolderMonitorDataSource import SubjectiveLocalFolderMonitorDataSource
    from SubjectiveTranscribeLocalVideoDataSource import SubjectiveTranscribeLocalVideoDataSource

    # Create pipeline
    my_pipeline_ds = SubjectiveDataSourcePipeline(name="MyVideoPipeline")

    # Add nodes
    my_pipeline_ds.add_node(
        node_id="folder_monitor",
        data_source_class=SubjectiveLocalFolderMonitorDataSource,
        params={
            "url": r"C:\Users\pablo\Videos",
            "connection_name": "VideoMonitor"
        }
    )

    # Filter for video files
    def video_filter(data):
        return (isinstance(data, dict) and
                data.get('event_type') == 'created' and
                str(data.get('path', '')).endswith(('.mp4', '.mkv')))

    my_pipeline_ds.add_node(
        node_id="transcriber",
        data_source_class=SubjectiveTranscribeLocalVideoDataSource,
        params={
            "context_dir": r"C:\brainboost\context"
        },
        dependencies=["folder_monitor"],
        filter_fn=video_filter
    )

    print(f"Pipeline configured: {my_pipeline_ds.get_name()}")
    print(f"Nodes: {list(my_pipeline_ds.nodes.keys())}")

    # Fetch works like any data source
    print("\nStarting pipeline...")
    try:
        my_pipeline_ds.fetch()
    except KeyboardInterrupt:
        print("\nStopping pipeline...")
        my_pipeline_ds.stop()


def example_3_pipeline_with_dependencies():
    """
    Example 3: Use a pipeline as a dependency for another data source

    This shows the true power - pipelines can now be used wherever
    a SubjectiveDataSource is expected.
    """
    print("=" * 70)
    print("Example 3: Pipeline as a Dependency")
    print("=" * 70)

    from SubjectiveLocalFolderMonitorDataSource import SubjectiveLocalFolderMonitorDataSource
    from SubjectiveTranscribeLocalVideoDataSource import SubjectiveTranscribeLocalVideoDataSource

    # Create a video transcription pipeline
    video_pipeline = SubjectiveDataSourcePipeline(name="VideoProcessing")

    video_pipeline.add_node(
        node_id="monitor",
        data_source_class=SubjectiveLocalFolderMonitorDataSource,
        params={"url": r"C:\Users\pablo\Videos"}
    )

    video_pipeline.add_node(
        node_id="transcriber",
        data_source_class=SubjectiveTranscribeLocalVideoDataSource,
        params={"context_dir": r"C:\brainboost\context"},
        dependencies=["monitor"]
    )

    # Now create ANOTHER pipeline that depends on the first one!
    # (This demonstrates that pipelines are now full SubjectiveDataSource objects)
    master_pipeline = SubjectiveDataSourcePipeline(
        name="MasterPipeline",
        dependency_data_sources=[video_pipeline]  # Pipeline as a dependency!
    )

    print(f"Master pipeline has {len(master_pipeline.dependency_data_sources)} dependency")
    print(f"Dependency: {master_pipeline.dependency_data_sources[0].get_name()}")

    # When master_pipeline starts, it can work with video_pipeline as a dependency
    print("\nThis demonstrates that pipelines are now composable!")


def example_4_subscribe_to_pipeline():
    """
    Example 4: Subscribe to pipeline updates

    Since pipelines now inherit from SubjectiveDataSource, you can
    subscribe to their updates.
    """
    print("=" * 70)
    print("Example 4: Subscribe to Pipeline Updates")
    print("=" * 70)

    # Create a subscriber class
    class MySubscriber:
        def update(self, data):
            print(f"Subscriber received update: {data}")

    pipeline_path = r"c:\brainboost\Subjective\com_subjective_userdata\com_subjective_pipelines\video_transcript_pipeline.pipe"

    # Create pipeline
    my_pipeline_ds = SubjectiveDataSourcePipeline(
        name="SubscribablePipeline",
        params={"pipeline_file": pipeline_path}
    )

    # Subscribe to pipeline updates
    subscriber = MySubscriber()
    my_pipeline_ds.subscribe(subscriber)

    print(f"Subscribed to pipeline: {my_pipeline_ds.get_name()}")
    print(f"Number of subscribers: {len(my_pipeline_ds.subscribers)}")

    # Now when any node in the pipeline emits data, subscribers will be notified
    print("\nStarting pipeline...")
    try:
        my_pipeline_ds.fetch()
    except KeyboardInterrupt:
        print("\nStopping pipeline...")
        my_pipeline_ds.stop()


def main():
    """
    Main function to select which example to run.
    """
    print("\n" + "=" * 70)
    print("SubjectiveDataSourcePipeline - New Capabilities Demo")
    print("=" * 70)
    print("\nThe pipeline now inherits from SubjectiveDataSource!")
    print("\nThis means:")
    print("  ✓ Can be loaded from .pipe files")
    print("  ✓ Can be used like any data source with fetch()")
    print("  ✓ Can have dependencies")
    print("  ✓ Can have subscribers")
    print("  ✓ Can be composed with other pipelines")
    print("=" * 70)

    print("\nSelect example to run:")
    print("1. Load pipeline from .pipe file")
    print("2. Configure pipeline programmatically")
    print("3. Use pipeline as a dependency (demo only)")
    print("4. Subscribe to pipeline updates")

    choice = input("\nEnter choice (1-4): ").strip()

    if choice == "1":
        example_1_from_pipe_file()
    elif choice == "2":
        example_2_programmatic_configuration()
    elif choice == "3":
        example_3_pipeline_with_dependencies()
    elif choice == "4":
        example_4_subscribe_to_pipeline()
    else:
        print("Invalid choice")


if __name__ == "__main__":
    main()
