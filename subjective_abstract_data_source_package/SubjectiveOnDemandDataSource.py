from __future__ import annotations

import base64
import mimetypes
import os
import queue
import threading
import time
import warnings
from typing import Any, Callable, Dict, List, Optional

from brainboost_data_source_logger_package.BBLogger import BBLogger

from .SubjectiveDataSource import SubjectiveDataSource


class SubjectiveOnDemandDataSource(SubjectiveDataSource):
    """
    Deprecated compatibility wrapper for legacy on-demand datasources.

    New datasources should inherit from SubjectiveDataSource directly and implement:
      - supports_chat()
      - handle_message(message, files=None)
    """

    _subjective_api_version = "v1"

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "SubjectiveOnDemandDataSource is deprecated. "
            "Use SubjectiveDataSource with supports_chat()/handle_message().",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
        self._response_callback: Optional[Callable[[Any], None]] = None
        self._message_queue: queue.Queue = queue.Queue()
        self._processing_thread: Optional[threading.Thread] = None
        self._processing_active = False
        self._shutdown_event = threading.Event()
        self._conversation_history: List[Dict[str, Any]] = []
        self._last_wait_log_time = 0.0
        self._wait_log_interval = 60.0
        self._async_mode = bool((self.params or {}).get("async_mode", True))
        self._max_history = int((self.params or {}).get("max_history", 100))
        BBLogger.log(f"SubjectiveOnDemandDataSource initialized: {self.get_name()}")

    def supports_chat(self) -> bool:
        return True

    def fetch(self):
        BBLogger.log(f"OnDemand fetch called for {self.get_name()}")
        if self._async_mode:
            self._start_processing_loop()
        return {"status": "ready", "datasource": self.get_name()}

    def handle_message(self, message: str, files: list | None = None) -> Any:
        payload_files = self._prepare_file_payloads(file_payloads=files)
        if payload_files:
            return self._handle_message({"content": message, "files": payload_files})
        return self._handle_message(message)

    def send_message(self, message: Any) -> Optional[Any]:
        BBLogger.log(
            f"[{self.get_name()}] send_message() called with type={type(message)} "
            f"value={str(message)[:200]}..."
        )
        self._add_to_history("user", message)
        if self._async_mode:
            self._message_queue.put(message)
            self._ensure_processing_loop()
            return None
        return self._handle_message(message)

    def send_message_with_files(
        self,
        message: Any,
        files: Optional[List[str]] = None,
        file_payloads: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional[Any]:
        payload_files = self._prepare_file_payloads(files, file_payloads)
        if not payload_files:
            return self.send_message(message)
        payload = {
            "content": message,
            "files": payload_files,
        }
        return self.send_message(payload)

    def set_response_callback(self, callback: Callable[[Any], None]):
        self._response_callback = callback
        BBLogger.log(f"Response callback set for {self.get_name()}")

    def get_conversation_history(self) -> List[Dict[str, Any]]:
        return list(self._conversation_history)

    def clear_conversation_history(self):
        self._conversation_history.clear()
        BBLogger.log(f"Conversation history cleared for {self.get_name()}")

    def stop(self):
        self._processing_active = False
        self._shutdown_event.set()
        if self._processing_thread:
            self._processing_thread.join(timeout=5.0)
            self._processing_thread = None
        BBLogger.log(f"Processing loop stopped for {self.get_name()}")

    def wait_until_stopped(self, timeout: Optional[float] = None) -> bool:
        return self._shutdown_event.wait(timeout=timeout)

    def wait_for_pending(self, timeout: Optional[float] = None):
        self._message_queue.join()

    def _add_to_history(self, role: str, content: Any):
        entry = {
            "role": role,
            "content": content,
            "timestamp": time.time(),
        }
        self._conversation_history.append(entry)
        if len(self._conversation_history) > self._max_history:
            self._conversation_history = self._conversation_history[-self._max_history :]

    def _handle_message(self, message: Any) -> Any:
        start_time = time.time()
        try:
            response = self._process_message(message)
            processing_time = time.time() - start_time
            self._total_processing_time += processing_time
            self._add_to_history("assistant", response)

            response_data = {
                "request": message,
                "response": response,
                "processing_time": processing_time,
                "timestamp": time.time(),
            }
            self.update(response_data)
            if self._response_callback:
                self._response_callback(response)
            return response
        except Exception as exc:
            error_response = {"error": str(exc), "message": message}
            self._add_to_history("error", error_response)
            if self._response_callback:
                self._response_callback(error_response)
            return error_response

    def _start_processing_loop(self):
        if self._processing_active:
            return

        self._processing_active = True
        self._shutdown_event.clear()
        self._processing_thread = threading.Thread(
            target=self._processing_loop,
            daemon=True,
            name=f"{self.get_name()}_processor",
        )
        self._processing_thread.start()
        BBLogger.log(f"Processing loop started for {self.get_name()}")

    def _ensure_processing_loop(self):
        if not self._processing_active or (
            self._processing_thread and not self._processing_thread.is_alive()
        ):
            self._start_processing_loop()

    def _processing_loop(self):
        BBLogger.log(f"[{self.get_name()}] Processing loop running")
        while self._processing_active:
            try:
                now = time.time()
                if now - self._last_wait_log_time >= self._wait_log_interval:
                    BBLogger.log(f"[{self.get_name()}] Waiting for message in queue...")
                    self._last_wait_log_time = now
                message = self._message_queue.get(timeout=1.0)
                self._handle_message(message)
                self._message_queue.task_done()
            except queue.Empty:
                continue
            except Exception as exc:
                BBLogger.log(f"[{self.get_name()}] Processing loop error: {exc}")

    def _prepare_file_payloads(
        self,
        files: Optional[List[str]] = None,
        file_payloads: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        payloads: List[Dict[str, Any]] = []

        if file_payloads:
            for payload in file_payloads:
                if isinstance(payload, dict):
                    payloads.append(dict(payload))

        if not files:
            return payloads

        max_bytes = int((self.params or {}).get("max_attachment_bytes", 5 * 1024 * 1024))
        max_text_chars = int((self.params or {}).get("max_attachment_text_chars", 20000))

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
                    "mime_type": mime_type,
                }

                with open(abs_path, "rb") as fh:
                    data = fh.read(max_bytes + 1)

                is_text = mime_type.startswith("text/") or os.path.splitext(abs_path)[1].lower() in (
                    ".txt",
                    ".md",
                    ".csv",
                    ".log",
                    ".json",
                    ".yaml",
                    ".yml",
                    ".xml",
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

    def _process_message(self, message: Any) -> Any:
        raise NotImplementedError("_process_message() must be implemented by subclasses")
