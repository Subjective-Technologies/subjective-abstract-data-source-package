import unittest

from subjective_abstract_data_source_package.SubjectivePipelineDataSource import SubjectiveDataSourcePipeline


class DummyDataSource:
    def __init__(self, name=None, dependency_data_sources=None, params=None, subscribers=None, **kwargs):
        self.name = name
        self.dependency_data_sources = dependency_data_sources or []
        self.params = params or {}
        self.subscribers = subscribers or []
        self.received = []

    def subscribe(self, subscriber):
        if subscriber not in self.subscribers:
            self.subscribers.append(subscriber)

    def notify(self, data):
        # Called when this data source wants to push updates to subscribers
        for s in self.subscribers:
            try:
                s.notify(data)
            except Exception:
                pass

    def process_input(self, data):
        # Record the exact payload received
        self.received.append(data)


class TestPipelineRedisRouting(unittest.TestCase):
    def test_redis_payload_routed_to_downstream_node_unchanged(self):
        pipeline = SubjectiveDataSourcePipeline(name="test_pipeline")

        # Add a monitor node that will be the emitting node
        pipeline.add_node(
            node_id='monitor',
            data_source_class=DummyDataSource,
            params={'connection_name': 'local_videos_monitor', 'redis_channel': 'file_events'}
        )

        # Add a processor node that depends on monitor
        pipeline.add_node(
            node_id='processor',
            data_source_class=DummyDataSource,
            dependencies=['monitor']
        )

        # Build pipeline (instantiate nodes)
        pipeline.build()

        # Sanity: ensure instances exist
        monitor_inst = pipeline.get_node_instance('monitor')
        processor_inst = pipeline.get_node_instance('processor')
        self.assertIsNotNone(monitor_inst)
        self.assertIsNotNone(processor_inst)

        # Create a fake Redis payload as emitted by LocalFolderMonitor
        payload = {
            'path': 'C:/videos/new.mp4',
            'dest_path': 'C:/videos/new.mp4',
            'is_directory': False,
            'event_type': 'created',
            'timestamp': '2026-01-13T12:00:00.000000',
            'folder_being_monitored': 'C:/videos',
            'connection_name': 'local_videos_monitor',
            'datasource_name': 'LocalFolderMonitor',
            'plugin_name': 'SubjectiveLocalFolderMonitorDataSource'
        }

        # Simulate incoming Redis event
        pipeline.handle_redis_payload(payload)

        # The processor should have received the exact same payload
        self.assertEqual(len(processor_inst.received), 1)
        self.assertEqual(processor_inst.received[0], payload)


if __name__ == '__main__':
    unittest.main()
