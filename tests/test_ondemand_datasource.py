import pytest
import time
import threading
from typing import Any
from unittest.mock import Mock, patch

from subjective_abstract_data_source_package import SubjectiveOnDemandDataSource


class MockOnDemandDataSource(SubjectiveOnDemandDataSource):
    """Mock implementation for testing."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.process_delay = kwargs.get("params", {}).get("process_delay", 0)

    def _process_message(self, message: Any) -> Any:
        if self.process_delay:
            time.sleep(self.process_delay)

        if isinstance(message, dict) and message.get("raise_error"):
            raise ValueError("Test error")

        return {"echo": message, "processed": True}

    def get_icon(self) -> str:
        return '<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/></svg>'

    def get_connection_data(self) -> dict:
        return {
            "connection_type": "ON_DEMAND",
            "fields": ["api_key", "endpoint"]
        }


class TestSubjectiveOnDemandDataSourceInit:
    """Test initialization of OnDemandDataSource."""

    def test_init_default_params(self):
        ds = MockOnDemandDataSource()
        assert ds._async_mode is True
        assert ds._max_history == 100
        assert ds._processing_active is False
        assert ds._response_callback is None

    def test_init_custom_params(self):
        ds = MockOnDemandDataSource(
            name="test_ds",
            params={"async_mode": False, "max_history": 50}
        )
        assert ds.name == "test_ds"
        assert ds._async_mode is False
        assert ds._max_history == 50

    def test_init_with_session(self):
        mock_session = Mock()
        ds = MockOnDemandDataSource(session=mock_session)
        assert ds.session is mock_session


class TestSendMessageSync:
    """Test synchronous message sending."""

    def test_send_message_sync_returns_response(self):
        ds = MockOnDemandDataSource(params={"async_mode": False})
        response = ds.send_message("Hello")
        assert response == {"echo": "Hello", "processed": True}

    def test_send_message_sync_with_dict(self):
        ds = MockOnDemandDataSource(params={"async_mode": False})
        message = {"text": "Hello", "user": "test"}
        response = ds.send_message(message)
        assert response["echo"] == message
        assert response["processed"] is True

    def test_send_message_sync_adds_to_history(self):
        ds = MockOnDemandDataSource(params={"async_mode": False})
        ds.send_message("First")
        ds.send_message("Second")

        history = ds.get_conversation_history()
        assert len(history) == 4  # 2 user + 2 assistant

        assert history[0]["role"] == "user"
        assert history[0]["content"] == "First"
        assert history[1]["role"] == "assistant"
        assert history[2]["role"] == "user"
        assert history[2]["content"] == "Second"


class TestSendMessageAsync:
    """Test asynchronous message sending."""

    def test_send_message_async_returns_none(self):
        ds = MockOnDemandDataSource(params={"async_mode": True})
        result = ds.send_message("Hello")
        assert result is None
        ds.wait_for_pending()
        ds.stop()

    def test_send_message_async_calls_callback(self):
        ds = MockOnDemandDataSource(params={"async_mode": True})
        responses = []

        def callback(response):
            responses.append(response)

        ds.set_response_callback(callback)
        ds.send_message("Hello")
        ds.wait_for_pending()
        ds.stop()

        assert len(responses) == 1
        assert responses[0] == {"echo": "Hello", "processed": True}

    def test_send_multiple_messages_async(self):
        ds = MockOnDemandDataSource(params={"async_mode": True})
        responses = []

        def callback(response):
            responses.append(response)

        ds.set_response_callback(callback)
        ds.send_message("First")
        ds.send_message("Second")
        ds.send_message("Third")
        ds.wait_for_pending()
        ds.stop()

        assert len(responses) == 3
        echoes = [r["echo"] for r in responses]
        assert "First" in echoes
        assert "Second" in echoes
        assert "Third" in echoes


class TestConversationHistory:
    """Test conversation history management."""

    def test_get_conversation_history_empty(self):
        ds = MockOnDemandDataSource(params={"async_mode": False})
        history = ds.get_conversation_history()
        assert history == []

    def test_get_conversation_history_returns_copy(self):
        ds = MockOnDemandDataSource(params={"async_mode": False})
        ds.send_message("Test")
        history1 = ds.get_conversation_history()
        history2 = ds.get_conversation_history()
        assert history1 is not history2

    def test_clear_conversation_history(self):
        ds = MockOnDemandDataSource(params={"async_mode": False})
        ds.send_message("Test")
        assert len(ds.get_conversation_history()) > 0

        ds.clear_conversation_history()
        assert len(ds.get_conversation_history()) == 0

    def test_max_history_limit(self):
        ds = MockOnDemandDataSource(params={"async_mode": False, "max_history": 5})

        for i in range(10):
            ds.send_message(f"Message {i}")

        history = ds.get_conversation_history()
        # Each message creates 2 entries (user + assistant)
        # With max_history=5, we should have at most 5 entries
        assert len(history) <= 5

    def test_history_entry_has_timestamp(self):
        ds = MockOnDemandDataSource(params={"async_mode": False})
        before = time.time()
        ds.send_message("Test")
        after = time.time()

        history = ds.get_conversation_history()
        assert "timestamp" in history[0]
        assert before <= history[0]["timestamp"] <= after


class TestErrorHandling:
    """Test error handling in message processing."""

    def test_error_in_process_message_sync(self):
        ds = MockOnDemandDataSource(params={"async_mode": False})
        response = ds.send_message({"raise_error": True})

        assert "error" in response
        assert response["error"] == "Test error"

    def test_error_adds_to_history(self):
        ds = MockOnDemandDataSource(params={"async_mode": False})
        ds.send_message({"raise_error": True})

        history = ds.get_conversation_history()
        error_entries = [h for h in history if h["role"] == "error"]
        assert len(error_entries) == 1

    def test_error_calls_callback_async(self):
        ds = MockOnDemandDataSource(params={"async_mode": True})
        responses = []

        def callback(response):
            responses.append(response)

        ds.set_response_callback(callback)
        ds.send_message({"raise_error": True})
        ds.wait_for_pending()
        ds.stop()

        assert len(responses) == 1
        assert "error" in responses[0]


class TestResponseCallback:
    """Test response callback functionality."""

    def test_set_response_callback(self):
        ds = MockOnDemandDataSource()
        callback = Mock()
        ds.set_response_callback(callback)
        assert ds._response_callback is callback

    def test_callback_exception_does_not_break_processing(self):
        ds = MockOnDemandDataSource(params={"async_mode": False})

        def bad_callback(response):
            raise RuntimeError("Callback error")

        ds.set_response_callback(bad_callback)

        # Should not raise, error is caught internally
        response = ds.send_message("Test")
        assert response is not None


class TestFetch:
    """Test fetch method for pipeline integration."""

    def test_fetch_returns_ready_status(self):
        ds = MockOnDemandDataSource(params={"async_mode": True})
        result = ds.fetch()
        assert result["status"] == "ready"
        assert "datasource" in result
        ds.stop()

    def test_fetch_starts_processing_loop(self):
        ds = MockOnDemandDataSource(params={"async_mode": True})
        assert ds._processing_active is False

        ds.fetch()
        assert ds._processing_active is True
        ds.stop()

    def test_fetch_sync_mode_does_not_start_loop(self):
        ds = MockOnDemandDataSource(params={"async_mode": False})
        ds.fetch()
        assert ds._processing_active is False


class TestProcessingLoop:
    """Test async processing loop."""

    def test_stop_terminates_loop(self):
        ds = MockOnDemandDataSource(params={"async_mode": True})
        ds.fetch()
        assert ds._processing_active is True

        ds.stop()
        assert ds._processing_active is False
        assert ds._processing_thread is None

    def test_processing_thread_is_daemon(self):
        ds = MockOnDemandDataSource(params={"async_mode": True})
        ds.fetch()

        assert ds._processing_thread is not None
        assert ds._processing_thread.daemon is True
        ds.stop()

    def test_wait_for_pending_blocks_until_complete(self):
        ds = MockOnDemandDataSource(
            params={"async_mode": True, "process_delay": 0.1}
        )
        responses = []

        def callback(response):
            responses.append(response)

        ds.set_response_callback(callback)
        ds.send_message("Test")

        # Response not immediate due to delay
        assert len(responses) == 0

        ds.wait_for_pending()
        assert len(responses) == 1
        ds.stop()


class TestSubscribers:
    """Test subscriber notification."""

    def test_update_notifies_subscribers(self):
        ds = MockOnDemandDataSource(params={"async_mode": False})
        subscriber = Mock()
        subscriber.notify = Mock()

        ds.subscribe(subscriber)
        ds.send_message("Test")

        subscriber.notify.assert_called()
        call_args = subscriber.notify.call_args[0][0]
        assert "request" in call_args
        assert "response" in call_args


class TestAbstractMethods:
    """Test that abstract methods are properly defined."""

    def test_get_icon_returns_svg(self):
        ds = MockOnDemandDataSource()
        icon = ds.get_icon()
        assert "<svg" in icon
        assert "</svg>" in icon

    def test_get_connection_data_returns_dict(self):
        ds = MockOnDemandDataSource()
        conn_data = ds.get_connection_data()
        assert isinstance(conn_data, dict)
        assert "connection_type" in conn_data
        assert conn_data["connection_type"] == "ON_DEMAND"


class TestContextOutput:
    """Test context output file generation."""

    @patch.object(MockOnDemandDataSource, '_write_context_output')
    def test_context_output_called_on_message(self, mock_write):
        ds = MockOnDemandDataSource(params={"async_mode": False})
        ds.send_message("Test")

        mock_write.assert_called()
        call_args = mock_write.call_args[0][0]
        assert "request" in call_args
        assert "response" in call_args
        assert "processing_time" in call_args


class TestIntegration:
    """Integration tests for complete workflows."""

    def test_full_sync_workflow(self):
        ds = MockOnDemandDataSource(
            name="integration_test",
            params={"async_mode": False}
        )

        # Send multiple messages
        r1 = ds.send_message("Hello")
        r2 = ds.send_message("World")

        assert r1["echo"] == "Hello"
        assert r2["echo"] == "World"

        # Check history
        history = ds.get_conversation_history()
        assert len(history) == 4

    def test_full_async_workflow(self):
        ds = MockOnDemandDataSource(
            name="async_integration_test",
            params={"async_mode": True}
        )

        responses = []
        ds.set_response_callback(lambda r: responses.append(r))

        # Start and send messages
        ds.fetch()
        ds.send_message("Async 1")
        ds.send_message("Async 2")

        # Wait and verify
        ds.wait_for_pending()
        ds.stop()

        assert len(responses) == 2
        history = ds.get_conversation_history()
        assert len(history) == 4


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
