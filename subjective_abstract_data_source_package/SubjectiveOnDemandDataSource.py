from abc import abstractmethod
from brainboost_data_source_logger_package.BBLogger import BBLogger
from .SubjectiveDataSource import SubjectiveDataSource
from collections.abc import Callable
from typing import Any, Optional, List, Dict
import base64
import mimetypes
import os
import threading
import queue
import time


class SubjectiveOnDemandDataSource(SubjectiveDataSource):
    """
    Abstract base class for on-demand request/response data sources.

    This data source type works like a conversation or chat interface,
    where messages are sent and responses are received asynchronously.

    Subclasses must implement:
        - _process_message(message): Handle the incoming message and return a response
        - get_icon(): Return SVG icon for the data source
        - get_connection_data(): Return connection metadata

    Usage:
        datasource = MyOnDemandDataSource(params={...})
        datasource.set_response_callback(my_callback)
        datasource.send_message("Hello how are you today")
    """

    def __init__(self, name=None, session=None, dependency_data_sources=None,
                 subscribers=None, params=None):
        # Compatibility shim for plugins that still call:
        #   super().__init__(name, params)
        # In that call shape, `params` lands in `session` and would otherwise be lost.
        if params is None and isinstance(session, dict) and dependency_data_sources is None and subscribers is None:
            params = session
            session = None
        super().__init__(
            name=name,
            session=session,
            dependency_data_sources=dependency_data_sources or [],
            subscribers=subscribers,
            params=params
        )

        # Response callback for async responses
        self._response_callback: Optional[Callable[[Any], None]] = None

        # Message queue for async processing
        self._message_queue: queue.Queue = queue.Queue()

        # Processing thread
        self._processing_thread: Optional[threading.Thread] = None
        self._processing_active: bool = False
        self._shutdown_event = threading.Event()
        self._last_wait_log_time: float = 0.0
        self._wait_log_interval: float = 60.0  # Log "waiting" at most every 60 seconds

        # Conversation history for chat-like interactions
        self._conversation_history: List[Dict[str, Any]] = []

        # Configuration
        self._async_mode: bool = self.params.get("async_mode", True)
        self._max_history: int = self.params.get("max_history", 100)

        BBLogger.log(f"SubjectiveOnDemandDataSource initialized: {self.get_name()}")

    def fetch(self):
        """
        For on-demand data sources, fetch starts the message processing loop.
        This allows the data source to be used in pipelines.
        """
        BBLogger.log(f"OnDemand fetch called for {self.get_name()}")
        if self._async_mode:
            self._start_processing_loop()
        return {"status": "ready", "datasource": self.get_name()}

    def send_message(self, message: Any) -> Optional[Any]:
        """
        Send a message to the data source and get a response.

        In async mode, the response will be delivered via the response callback.
        In sync mode, this method blocks until the response is ready.

        Args:
            message: The message to send (string, dict, or any serializable object)

        Returns:
            In sync mode: The response from the data source
            In async mode: None (response delivered via callback)
        """
        BBLogger.log(f"[{self.get_name()}] send_message() called with message type: {type(message)}, value: {str(message)[:200]}...")

        # Add to conversation history
        self._add_to_history("user", message)

        if self._async_mode:
            BBLogger.log(f"[{self.get_name()}] Async mode: queueing message for processing")
            # Queue the message for async processing
            self._message_queue.put(message)
            self._ensure_processing_loop()
            BBLogger.log(f"[{self.get_name()}] Message queued, returning None (response via callback)")
            return None
        else:
            BBLogger.log(f"[{self.get_name()}] Sync mode: processing message immediately")
            # Process synchronously
            response = self._handle_message(message)
            BBLogger.log(f"[{self.get_name()}] Sync mode: response received: {str(response)[:200]}...")
            return response

    def send_message_with_files(
        self,
        message: Any,
        files: Optional[List[str]] = None,
        file_payloads: Optional[List[Dict[str, Any]]] = None
    ) -> Optional[Any]:
        """
        Send a message along with file attachments.

        Args:
            message: The message to send (string, dict, or any serializable object)
            files: List of file paths to attach
            file_payloads: Pre-built file payloads (dicts) to attach

        Returns:
            In sync mode: The response from the data source
            In async mode: None (response delivered via callback)
        """
        payload_files = self._prepare_file_payloads(files, file_payloads)
        if not payload_files:
            return self.send_message(message)

        payload = {
            "content": message,
            "files": payload_files
        }
        return self.send_message(payload)

    def set_response_callback(self, callback: Callable[[Any], None]):
        """
        Set a callback function to receive responses.

        The callback will be called with the response data whenever
        a message is processed.

        Args:
            callback: Function that accepts response data
        """
        self._response_callback = callback
        BBLogger.log(f"Response callback set for {self.get_name()}")

    def get_conversation_history(self) -> List[Dict[str, Any]]:
        """
        Get the conversation history.

        Returns:
            List of conversation entries with role, content, and timestamp
        """
        return list(self._conversation_history)

    def clear_conversation_history(self):
        """Clear the conversation history."""
        self._conversation_history.clear()
        BBLogger.log(f"Conversation history cleared for {self.get_name()}")

    def _add_to_history(self, role: str, content: Any):
        """Add an entry to the conversation history."""
        entry = {
            "role": role,
            "content": content,
            "timestamp": time.time()
        }
        self._conversation_history.append(entry)

        # Trim history if needed
        if len(self._conversation_history) > self._max_history:
            self._conversation_history = self._conversation_history[-self._max_history:]

    def _handle_message(self, message: Any) -> Any:
        """
        Handle a message by processing it and delivering the response.

        Args:
            message: The message to process

        Returns:
            The response from _process_message
        """
        start_time = time.time()
        BBLogger.log(f"[{self.get_name()}] _handle_message() called with message: {str(message)[:200]}...")

        try:
            # Process the message (implemented by subclass)
            BBLogger.log(f"[{self.get_name()}] Calling _process_message() to process the request")
            response = self._process_message(message)
            BBLogger.log(f"[{self.get_name()}] _process_message() returned response type: {type(response)}, value: {str(response)[:300]}...")

            # Track processing time
            processing_time = time.time() - start_time
            self._total_processing_time += processing_time

            # Add response to history
            self._add_to_history("assistant", response)

            # Notify subscribers via update mechanism
            response_data = {
                "request": message,
                "response": response,
                "processing_time": processing_time,
                "timestamp": time.time()
            }
            self.update(response_data)

            # Call response callback if set
            if self._response_callback:
                BBLogger.log(f"[{self.get_name()}] Response callback is set, calling it with response")
                try:
                    self._response_callback(response)
                    BBLogger.log(f"[{self.get_name()}] Response callback completed successfully")
                except Exception as e:
                    import traceback
                    BBLogger.log(f"[{self.get_name()}] Response callback failed: {e}\n{traceback.format_exc()}")
            else:
                BBLogger.log(f"[{self.get_name()}] WARNING: No response callback set - response will not be delivered")

            BBLogger.log(f"[{self.get_name()}] Message processed successfully in {processing_time:.2f}s")
            return response

        except Exception as e:
            import traceback
            BBLogger.log(f"[{self.get_name()}] Error processing message: {e}\n{traceback.format_exc()}")
            error_response = {"error": str(e), "message": message}
            self._add_to_history("error", error_response)

            if self._response_callback:
                BBLogger.log(f"[{self.get_name()}] Calling error callback with error response")
                try:
                    self._response_callback(error_response)
                    BBLogger.log(f"[{self.get_name()}] Error callback completed")
                except Exception as cb_error:
                    import traceback
                    BBLogger.log(f"[{self.get_name()}] Error callback failed: {cb_error}\n{traceback.format_exc()}")
            else:
                BBLogger.log(f"[{self.get_name()}] WARNING: No response callback set - error response will not be delivered")

            return error_response

    def _start_processing_loop(self):
        """Start the async message processing loop."""
        if self._processing_active:
            return

        self._processing_active = True
        self._shutdown_event.clear()
        self._processing_thread = threading.Thread(
            target=self._processing_loop,
            daemon=True,
            name=f"{self.get_name()}_processor"
        )
        self._processing_thread.start()
        BBLogger.log(f"Processing loop started for {self.get_name()}")

    def _ensure_processing_loop(self):
        """Ensure the processing loop is running."""
        if not self._processing_active or (
            self._processing_thread and not self._processing_thread.is_alive()
        ):
            self._start_processing_loop()

    def _processing_loop(self):
        """Main loop for async message processing."""
        BBLogger.log(f"[{self.get_name()}] Processing loop running for {self.get_name()}")

        while self._processing_active:
            try:
                # Wait for a message with timeout (log at most every _wait_log_interval seconds)
                now = time.time()
                if now - self._last_wait_log_time >= self._wait_log_interval:
                    BBLogger.log(f"[{self.get_name()}] Waiting for message in queue...")
                    self._last_wait_log_time = now
                message = self._message_queue.get(timeout=1.0)
                BBLogger.log(f"[{self.get_name()}] Message retrieved from queue: {str(message)[:200]}...")
                self._handle_message(message)
                self._message_queue.task_done()
                BBLogger.log(f"[{self.get_name()}] Message processing completed, waiting for next message")
            except queue.Empty:
                # No message, continue waiting
                continue
            except Exception as e:
                import traceback
                BBLogger.log(f"[{self.get_name()}] Processing loop error: {e}\n{traceback.format_exc()}")

    def stop(self):
        """Stop the message processing loop."""
        self._processing_active = False
        self._shutdown_event.set()
        if self._processing_thread:
            self._processing_thread.join(timeout=5.0)
            self._processing_thread = None
        BBLogger.log(f"Processing loop stopped for {self.get_name()}")

    def wait_until_stopped(self, timeout: Optional[float] = None) -> bool:
        """
        Block until the data source is stopped.

        Args:
            timeout: Maximum time to wait in seconds (None for no limit)

        Returns:
            True if stop was observed, False if the timeout elapsed.
        """
        return self._shutdown_event.wait(timeout=timeout)

    def wait_for_pending(self, timeout: Optional[float] = None):
        """
        Wait for all pending messages to be processed.

        Args:
            timeout: Maximum time to wait in seconds (None for no limit)
        """
        self._message_queue.join()

    def _prepare_file_payloads(
        self,
        files: Optional[List[str]] = None,
        file_payloads: Optional[List[Dict[str, Any]]] = None
    ) -> List[Dict[str, Any]]:
        payloads: List[Dict[str, Any]] = []

        if file_payloads:
            for payload in file_payloads:
                if isinstance(payload, dict):
                    payloads.append(dict(payload))

        if not files:
            return payloads

        max_bytes = int(self.params.get("max_attachment_bytes", 5 * 1024 * 1024))
        max_text_chars = int(self.params.get("max_attachment_text_chars", 20000))

        for path in files:
            if not path:
                continue
            try:
                abs_path = os.path.abspath(path)
                if not os.path.isfile(abs_path):
                    continue
                size = os.path.getsize(abs_path)
                mime_type, _ = mimetypes.guess_type(abs_path)
                mime_type = mime_type or "application/octet-stream"

                payload: Dict[str, Any] = {
                    "name": os.path.basename(abs_path),
                    "path": abs_path,
                    "size": size,
                    "mime_type": mime_type
                }

                with open(abs_path, "rb") as f:
                    data = f.read(max_bytes + 1)

                is_text = mime_type.startswith("text/") or os.path.splitext(abs_path)[1].lower() in (
                    ".txt", ".md", ".csv", ".log", ".json", ".yaml", ".yml", ".xml"
                )

                if is_text:
                    text = data.decode("utf-8", errors="ignore")
                    truncated = len(text) > max_text_chars or len(data) > max_bytes
                    if truncated:
                        text = text[:max_text_chars]
                    payload["text"] = text
                    payload["text_truncated"] = truncated
                else:
                    truncated = len(data) > max_bytes
                    if truncated:
                        data = data[:max_bytes]
                    payload["data_base64"] = base64.b64encode(data).decode("ascii")
                    payload["data_truncated"] = truncated

                payloads.append(payload)
            except Exception:
                continue

        return payloads

    # === Abstract methods that must be implemented by subclasses ===

    @abstractmethod
    def _process_message(self, message: Any) -> Any:
        """
        Process an incoming message and return a response.

        This method must be implemented by subclasses to define
        the actual message processing logic.

        Args:
            message: The incoming message to process

        Returns:
            The response to send back
        """
        pass

    @abstractmethod
    def get_icon(self) -> str:
        """Return the SVG code for the data source icon."""
        pass

    @abstractmethod
    def get_connection_data(self) -> dict:
        """
        Return the connection type and required fields for this data source.

        Example:
        {
            "connection_type": "ON_DEMAND",
            "fields": ["api_endpoint", "api_key"]
        }
        """
        pass
