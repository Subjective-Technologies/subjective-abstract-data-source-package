from __future__ import annotations

from abc import ABC
from collections.abc import Iterable
from datetime import datetime
import inspect
import json
import os
import time
from typing import Any
import warnings

from brainboost_configuration_package.BBConfig import BBConfig
from brainboost_data_source_logger_package.BBLogger import BBLogger

from .temp_storage import build_connection_tmp_dir, build_process_tmp_root


_DEPRECATED_V1_WRAPPERS = {
    "SubjectiveOnDemandDataSource",
    "SubjectiveRealTimeDataSource",
    "SubjectivePipelineDataSource",
}


class SubjectiveDataSource(ABC):
    """
    Subjective datasource base class.

    The v2 API is:
      - connection_schema()
      - run(request)

    The v1 API remains supported for backwards compatibility:
      - get_connection_data()
      - fetch()
      - get_icon()
    """

    _subjective_api_version = "v1"

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        is_wrapper_class = cls.__name__ in _DEPRECATED_V1_WRAPPERS
        declares_v2 = any(
            name in cls.__dict__
            for name in ("connection_schema", "run", "stream", "handle_message")
        )
        declares_v1 = any(
            name in cls.__dict__
            for name in ("get_connection_data", "fetch", "get_icon", "_process_message")
        )
        inherited_version = getattr(cls, "_subjective_api_version", "v1")
        if is_wrapper_class:
            cls._subjective_api_version = "v1"
        elif declares_v2 and not declares_v1:
            cls._subjective_api_version = "v2"
        elif declares_v1:
            cls._subjective_api_version = "v1"
        else:
            cls._subjective_api_version = inherited_version

        is_candidate_v2 = (
            not is_wrapper_class
            and "fetch" not in cls.__dict__
            and "get_connection_data" not in cls.__dict__
            and cls.api_version() == "v2"
        )
        if is_candidate_v2 and not inspect.isabstract(cls):
            if "run" not in cls.__dict__:
                raise TypeError(
                    f"{cls.__name__} must implement run(self, request) "
                    "— see 26_datasource_api_v2_refactor.md section 2.1"
                )
            if "connection_schema" not in cls.__dict__:
                raise TypeError(
                    f"{cls.__name__} must implement connection_schema(cls) "
                    "— see 26_datasource_api_v2_refactor.md section 2.1"
                )

        original_fetch = cls.__dict__.get("fetch")
        if original_fetch is None or getattr(original_fetch, "_subjective_wrapped_fetch", False):
            return

        def wrapped_fetch(self, *args, **kwargs):
            result = original_fetch(self, *args, **kwargs)
            try:
                self._write_context_output_from_fetch(result)
            except Exception as exc:
                BBLogger.log(f"Failed writing fetch output for {self.__class__.__name__}: {exc}")
            return result

        wrapped_fetch._subjective_wrapped_fetch = True  # type: ignore[attr-defined]
        cls.fetch = wrapped_fetch

    def __init__(self, *args, **kwargs):
        v1_kwarg_keys = {
            "name",
            "session",
            "dependency_data_sources",
            "subscribers",
            "params",
        }

        if "connection" in kwargs:
            connection = kwargs.pop("connection")
            config = kwargs.pop("config", None)
            self._init_v2(connection=connection, config=config)
            return
        if "name" in kwargs or len(args) >= 3:
            self._init_v1(*args, **kwargs)
            return
        if len(args) == 1 and isinstance(args[0], dict) and not any(key in kwargs for key in v1_kwarg_keys):
            self._init_v2(connection=args[0], config=kwargs.pop("config", None))
            return
        if (
            len(args) == 2
            and isinstance(args[0], dict)
            and isinstance(args[1], dict)
            and not any(key in kwargs for key in v1_kwarg_keys)
        ):
            self._init_v2(connection=args[0], config=args[1])
            return
        if not args and set(kwargs.keys()) <= {"params"}:
            self._init_v1(*args, **kwargs)
            return

        self._init_v1(*args, **kwargs)

    @classmethod
    def api_version(cls) -> str:
        return getattr(cls, "_subjective_api_version", "v1")

    @classmethod
    def is_v2_class(cls) -> bool:
        return cls.api_version() == "v2"

    @classmethod
    def connection_schema(cls) -> dict:
        if cls.is_v2_class():
            raise NotImplementedError(f"{cls.__name__}.connection_schema() is not implemented")

        definition = cls._legacy_connection_definition()
        return cls._connection_definition_to_schema(definition)

    @classmethod
    def request_schema(cls) -> dict:
        return {}

    @classmethod
    def output_schema(cls) -> dict:
        return {}

    @classmethod
    def icon(cls) -> str:
        if cls.is_v2_class():
            return ""

        instance = cls._extract_v1_metadata()
        if instance is None:
            return ""
        try:
            return instance.get_icon() or ""
        except Exception:
            try:
                fallback_instance = cls(params={})
                return fallback_instance.get_icon() or ""
            except Exception:
                return ""

    def run(self, request: dict) -> Any:
        if not self.__class__.is_v2_class():
            merged = dict(self.params or {})
            if isinstance(request, dict):
                merged.update(request)
            self.params = merged
            return self.fetch()
        raise NotImplementedError(f"{self.__class__.__name__}.run() is not implemented")

    def supports_streaming(self) -> bool:
        return False

    def stream(self, request: dict):
        raise NotImplementedError("Streaming not supported by this datasource")

    def supports_chat(self) -> bool:
        return False

    def handle_message(self, message: str, files: list | None = None) -> Any:
        if not self.__class__.is_v2_class() and hasattr(self, "send_message"):
            if files and hasattr(self, "send_message_with_files"):
                return self.send_message_with_files(message, file_payloads=files)
            return self.send_message(message)
        raise NotImplementedError("Chat not supported by this datasource")

    def fetch(self):
        request = {}
        if isinstance(self._config, dict):
            config_request = self._config.get("request_data")
            if isinstance(config_request, dict):
                request.update(config_request)
        if isinstance(self.params, dict):
            request.update(self.params)
        return self.run(request)

    def get_icon(self) -> str:
        return self.__class__.icon()

    def get_connection_data(self) -> dict:
        schema = self.__class__.connection_schema()
        fields = self._schema_to_field_list(schema)
        return {
            "connection_type": self.get_data_source_type_name().upper() or self.__class__.__name__.upper(),
            "connection_form": schema,
            "fields": fields,
        }

    @property
    def input_dir(self) -> str:
        return str(self._config.get("input_dir", "") or "")

    @property
    def output_dir(self) -> str:
        output_dir = str(self._config.get("output_dir", "") or "")
        if output_dir:
            return output_dir
        return self._resolve_context_path()

    @property
    def scratch_dir(self) -> str:
        return str(self._config.get("scratch_dir", "") or "")

    @property
    def connection_name(self) -> str:
        return str(self._config.get("connection_name", "") or self._get_runtime_connection_name())

    @connection_name.setter
    def connection_name(self, value):
        if not isinstance(getattr(self, "_config", None), dict):
            self._config = {}
        self._config["connection_name"] = str(value) if value else ""

    # ==================================================================
    # v1 backward-compatibility methods
    # These exist only for legacy SubjectiveDataSource v1 subclasses.
    # v2 datasources should not use these; the framework handles
    # subscribers, context output, and progress externally.
    # ==================================================================

    def start(self):
        self._warn_if_v2_uses_v1_method("start")
        for ds in self.dependency_data_sources:
            ds.subscribe(self)
            ds.start()

    def update(self, data):
        self._warn_if_v2_uses_v1_method("update")
        self.increment_processed_items()
        self._emit_progress()
        try:
            self._write_context_output(data)
        except Exception as exc:
            BBLogger.log(f"Failed writing context output: {exc}")
        for subscriber in self.subscribers:
            notify = getattr(subscriber, "notify", None) or getattr(subscriber, "update", None)
            if callable(notify):
                notify(data)

    def send_notification(self, notification_data):
        """
        Send a notification to all subscribers with the provided data.
        Adds a timestamp if not present, then delegates to update().

        This is the preferred method for datasources to emit real-time events.
        """
        try:
            if isinstance(notification_data, dict) and "timestamp" not in notification_data:
                notification_data["timestamp"] = time.time()
            self.update(notification_data)
        except Exception as exc:
            BBLogger.log(f"Error sending notification: {exc}")

    def send_redis_notification(self, channel, notification_data):
        """
        Publish notification data to a Redis channel.

        Uses BBConfig for Redis connection details. Falls back gracefully
        if Redis is not available (e.g., during testing or isolated execution).
        """
        try:
            import redis as redis_lib

            redis_host = "localhost"
            redis_port = 6379
            try:
                redis_host = str(BBConfig.get("REDIS_SERVER_IP", "localhost"))
            except Exception:
                pass
            try:
                redis_port = int(BBConfig.get("REDIS_SERVER_PORT", 6379))
            except Exception:
                pass

            client = redis_lib.Redis(host=redis_host, port=redis_port, db=0)

            if isinstance(notification_data, dict) and "timestamp" not in notification_data:
                notification_data["timestamp"] = time.time()

            client.publish(channel, json.dumps(notification_data, default=str))
            BBLogger.log(f"Sent Redis notification to channel '{channel}'")

        except ImportError:
            BBLogger.log("Redis not available for notifications")
        except Exception as exc:
            BBLogger.log(f"Error sending Redis notification: {exc}")

    def subscribe(self, subscriber):
        self._warn_if_v2_uses_v1_method("subscribe")
        if subscriber not in self.subscribers:
            self.subscribers.append(subscriber)
            BBLogger.log(f"Subscriber {subscriber} added.")

    def get_name(self):
        class_name = self.__class__.__name__
        runtime_name = self.connection_name or self.name
        if runtime_name:
            return f"{runtime_name}_{class_name}"
        return class_name

    def get_data_source_type_name(self):
        class_name = self.__class__.__name__
        data_source_type_name = (
            class_name
            .replace("Subjective", "")
            .replace("RealTimeDataSource", "")
            .replace("OnDemandDataSource", "")
            .replace("PipelineDataSource", "Pipeline")
            .replace("DataSource", "")
        )
        return data_source_type_name

    def _sanitize_label(self, value):
        return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(value))

    def _get_connection_label(self):
        connection_name = self.connection_name
        if not connection_name and isinstance(self.params, dict):
            connection_name = (
                self.params.get("connection_name")
                or self.params.get("connectionName")
                or self.params.get("connection")
            )
        if not connection_name:
            return None
        connection_name = str(connection_name).strip()
        if not connection_name:
            return None
        return self._sanitize_label(connection_name)

    def _get_runtime_connection_name(self):
        if isinstance(self._config, dict):
            connection_name = self._config.get("connection_name")
            if connection_name:
                return str(connection_name).strip()

        if isinstance(self.params, dict):
            connection_name = (
                self.params.get("connection_name")
                or self.params.get("connectionName")
                or self.params.get("connection")
            )
            if connection_name:
                return str(connection_name).strip()

        for key in ("CONNECTION_NAME", "connection_name"):
            value = self._safe_get_config_value(key)
            if value:
                return str(value).strip()

        for key in ("CONNECTION_NAME", "connection_name"):
            value = os.environ.get(key)
            if value:
                return str(value).strip()

        return ""

    def _get_log_process_name(self):
        datasource_name = self.get_data_source_type_name()
        connection_label = self._get_connection_label()
        if connection_label:
            return f"{datasource_name}-{connection_label}"
        return datasource_name

    def _safe_get_config_value(self, key):
        try:
            return BBConfig.get(key)
        except Exception:
            return None

    def _normalize_context_path(self, path_value):
        path_text = "" if path_value is None else str(path_value).strip()
        if not path_text:
            return ""
        path_text = os.path.expandvars(os.path.expanduser(path_text))
        if not os.path.isabs(path_text):
            path_text = os.path.abspath(path_text)
        return os.path.normpath(path_text)

    def _ensure_context_params(self):
        if not isinstance(self.params, dict):
            return
        context_value = None
        for key in ("context_dir", "TARGET_DIRECTORY", "target_directory", "CONTEXT_DIR"):
            value = self.params.get(key)
            if value:
                context_value = value
                break
        if not context_value and isinstance(self._config, dict):
            context_value = (
                self._config.get("context_dir")
                or self._config.get("TARGET_DIRECTORY")
                or self._config.get("target_directory")
                or self._config.get("CONTEXT_DIR")
            )
        if not context_value:
            context_value = self._get_default_context_path()
        if not context_value:
            return
        for key in ("context_dir", "TARGET_DIRECTORY", "target_directory", "CONTEXT_DIR"):
            if not self.params.get(key):
                self.params[key] = context_value

    def _get_default_context_path(self):
        context_storage = self._safe_get_config_value("CONTEXT_STORAGE")
        normalized_context_storage = self._normalize_context_path(context_storage)
        if normalized_context_storage:
            return normalized_context_storage

        userdata_path = self._safe_get_config_value("USERDATA_PATH")
        normalized_userdata_path = self._normalize_context_path(userdata_path)
        if normalized_userdata_path:
            return self._normalize_context_path(
                os.path.join(normalized_userdata_path, "com_subjective_context")
            )

        return self._normalize_context_path(
            os.path.join("~", ".Subjective", "com_subjective_userdata", "com_subjective_context")
        )

    def _resolve_context_path(self):
        self._warn_if_v2_uses_v1_method("_resolve_context_path")
        if isinstance(self._config, dict):
            for key in ("output_dir", "context_dir", "TARGET_DIRECTORY", "target_directory", "CONTEXT_DIR"):
                value = self._config.get(key)
                if value:
                    normalized = self._normalize_context_path(value)
                    if normalized:
                        return normalized
        if isinstance(self.params, dict):
            for key in ("TARGET_DIRECTORY", "target_directory", "context_dir", "CONTEXT_DIR"):
                value = self.params.get(key)
                if value:
                    normalized = self._normalize_context_path(value)
                    if normalized:
                        return normalized
        return self._get_default_context_path()

    def get_connection_temp_dir(self):
        tmp_root = ""
        if isinstance(self._config, dict):
            tmp_root = str(
                self._config.get("ds_connection_tmp_space")
                or self._config.get("DS_CONNECTION_TMP_SPACE")
                or ""
            ).strip()
        if not tmp_root and isinstance(self.params, dict):
            tmp_root = str(
                self.params.get("ds_connection_tmp_space")
                or self.params.get("DS_CONNECTION_TMP_SPACE")
                or ""
            ).strip()
        if not tmp_root:
            tmp_root = str(self._safe_get_config_value("DS_CONNECTION_TMP_SPACE") or "").strip()
        if not tmp_root:
            tmp_root = str(os.environ.get("DS_CONNECTION_TMP_SPACE") or "").strip()

        connection_name = self._get_runtime_connection_name() or f"connection_{os.getpid()}"

        if not tmp_root:
            fallback_root = build_process_tmp_root()
            fallback_dir = build_connection_tmp_dir(fallback_root, connection_name)
            os.makedirs(fallback_dir, exist_ok=True)
            BBLogger.log(
                "WARNING: [SubjectiveDataSource] DS_CONNECTION_TMP_SPACE missing; "
                f"falling back to per-process temp dir: {fallback_dir}"
            )
            return fallback_dir

        target_dir = build_connection_tmp_dir(tmp_root, connection_name)
        os.makedirs(target_dir, exist_ok=True)
        BBLogger.log(f"DEBUG: [SubjectiveDataSource] Connection temp dir: {target_dir}")
        return target_dir

    def _warn_if_v2_uses_v1_method(self, method_name: str):
        if getattr(self, "_subjective_api_version", "v1") != "v1":
            warnings.warn(
                f"{self.__class__.__name__}.{method_name}() is a v1 method. "
                "v2 datasources should not call it directly.",
                DeprecationWarning,
                stacklevel=2,
            )

    def _write_context_output(self, data):
        self._warn_if_v2_uses_v1_method("_write_context_output")
        context_dir = self._resolve_context_path()
        if not context_dir:
            return ""
        os.makedirs(context_dir, exist_ok=True)

        datasource_name = self.get_data_source_type_name()
        output_format = self._safe_get_config_value("CONTEXT_OUTPUT_FORMAT") or "json"
        name_convention = self._safe_get_config_value("CONTEXT_FILE_NAME_CONVENTION")
        if not name_convention:
            name_convention = "YYYY_MM_DD_HH_MM_SS-[ds_name]-context.${CONTEXT_OUTPUT_FORMAT}"
        connection_label = self._get_connection_label()
        if connection_label and "[connection_name]" not in name_convention:
            datasource_name = f"{datasource_name}-{connection_label}"
        ts = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        filename = name_convention.replace("YYYY_MM_DD_HH_MM_SS", ts)
        filename = filename.replace("[ds_name]", datasource_name)
        if "[connection_name]" in filename:
            filename = filename.replace("[connection_name]", connection_label or "unknown")
        filename = filename.replace("${CONTEXT_OUTPUT_FORMAT}", output_format)
        path = os.path.join(context_dir, filename)

        payload = data
        if isinstance(payload, bytes):
            with open(path, "wb") as fh:
                fh.write(payload)
            return path
        if not isinstance(payload, (dict, list)):
            payload = {"value": payload}

        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=str)
        try:
            os.chmod(path, 0o666)
        except Exception:
            pass
        return path

    def _write_context_output_from_fetch(self, result):
        self._warn_if_v2_uses_v1_method("_write_context_output_from_fetch")
        if result is None:
            return
        if isinstance(result, (dict, list, str, bytes)):
            self._write_context_output(result)
            return
        if isinstance(result, Iterable):
            for item in result:
                self._write_context_output(item)
            return
        self._write_context_output(result)

    def set_progress_callback(self, callback):
        self._warn_if_v2_uses_v1_method("set_progress_callback")
        self.progress_callback = callback
        BBLogger.log(f"Progress callback set to: {callback}")

    def set_status_callback(self, callback):
        self._warn_if_v2_uses_v1_method("set_status_callback")
        self.status_callback = callback
        BBLogger.log(f"Status callback set to: {callback}")

    def estimated_remaining_time(self):
        self._warn_if_v2_uses_v1_method("estimated_remaining_time")
        remaining_to_process = self.remaining_to_process()
        if remaining_to_process is None:
            BBLogger.log("Total items unknown; cannot estimate remaining time")
            return None
        remaining = remaining_to_process * self.average_time_per_item()
        BBLogger.log(f"Estimated remaining time: {remaining} seconds")
        return remaining

    def get_total_to_process(self):
        self._warn_if_v2_uses_v1_method("get_total_to_process")
        BBLogger.log(f"Getting total items to process: {self._total_items}")
        return self._total_items

    def get_total_processed(self):
        self._warn_if_v2_uses_v1_method("get_total_processed")
        BBLogger.log(f"Getting total processed items: {self._processed_items}")
        return self._processed_items

    def get_total_processing_time(self):
        self._warn_if_v2_uses_v1_method("get_total_processing_time")
        BBLogger.log(f"Getting total processing time: {self._total_processing_time} seconds")
        return self._total_processing_time

    def remaining_to_process(self):
        self._warn_if_v2_uses_v1_method("remaining_to_process")
        total = self.get_total_to_process()
        if total <= 0:
            BBLogger.log("Total items unknown; remaining items cannot be computed")
            return None
        remaining = total - self.get_total_processed()
        BBLogger.log(f"Remaining items to process: {remaining}")
        return remaining

    def increment_processed_items(self):
        self._warn_if_v2_uses_v1_method("increment_processed_items")
        new_processed = self.get_total_processed() + 1
        self.set_processed_items(new_processed)
        BBLogger.log(f"Incremented processed items to: {new_processed}")

    def set_total_items(self, total_items):
        self._warn_if_v2_uses_v1_method("set_total_items")
        self._total_items = total_items
        BBLogger.log(f"Set total items to: {total_items}")
        if self._progress_enabled:
            self._ensure_progress_bar(restart_if_unknown=True)

    def set_processed_items(self, processed_items):
        self._warn_if_v2_uses_v1_method("set_processed_items")
        self._processed_items = processed_items
        BBLogger.log(f"Set processed items to: {processed_items}")

    def set_total_processing_time(self, total_processing_time):
        self._warn_if_v2_uses_v1_method("set_total_processing_time")
        self._total_processing_time = total_processing_time
        BBLogger.log(f"Set total processing time to: {total_processing_time} seconds")

    def set_fetch_completed(self, fetch_completed=False):
        self._warn_if_v2_uses_v1_method("set_fetch_completed")
        self._fetch_completed = fetch_completed
        BBLogger.log(f"Set fetch completed to: {fetch_completed}")
        if fetch_completed:
            self._close_progress_bar()

    def average_time_per_item(self):
        self._warn_if_v2_uses_v1_method("average_time_per_item")
        if self.get_total_processed() == 0:
            BBLogger.log("No items processed yet; average time per item is 0.0 seconds")
            return 0.0
        avg = self.get_total_processing_time() / self.get_total_processed()
        BBLogger.log(f"Calculated average time per item: {avg} seconds")
        return avg

    def _emit_progress(self):
        self._warn_if_v2_uses_v1_method("_emit_progress")
        if self._progress_enabled:
            self._ensure_progress_bar()
            if self._progress_bar:
                try:
                    self._progress_bar()
                except Exception as exc:
                    BBLogger.log(f"Progress bar update failed: {exc}")
        total_items = self.get_total_to_process()
        processed_items = self.get_total_processed()
        estimated_time = None if total_items <= 0 else self.estimated_remaining_time()
        if self.progress_callback:
            try:
                self.progress_callback(self.get_name(), total_items, processed_items, estimated_time)
            except Exception as exc:
                BBLogger.log(f"Progress callback failed: {exc}")

    def enable_progress_bar(self, enabled=True):
        self._warn_if_v2_uses_v1_method("enable_progress_bar")
        self._progress_enabled = bool(enabled)
        if self._progress_enabled:
            self._ensure_progress_bar()
        else:
            self._close_progress_bar()

    def _configure_progress_from_params(self):
        self._warn_if_v2_uses_v1_method("_configure_progress_from_params")
        progress_flag = False
        if isinstance(self.params, dict):
            progress_flag = bool(self.params.get("progress") or self.params.get("--progress"))
        elif isinstance(self.params, (list, tuple, set)):
            progress_flag = "--progress" in self.params or "progress" in self.params
        if progress_flag:
            self.enable_progress_bar(True)

    def _ensure_progress_bar(self, restart_if_unknown=False):
        self._warn_if_v2_uses_v1_method("_ensure_progress_bar")
        if not self._progress_enabled:
            return
        try:
            from alive_progress import alive_bar
        except Exception as exc:
            BBLogger.log(f"alive-progress not available: {exc}")
            self._progress_enabled = False
            return

        total = self.get_total_to_process()
        total = None if total <= 0 else total

        if self._progress_bar_ctx:
            if not restart_if_unknown:
                return
            if self._progress_bar_total is not None or total is None:
                return
            if self.get_total_processed() > 0:
                return
            self._close_progress_bar()

        self._progress_bar_ctx = alive_bar(total, title=self.get_name())
        self._progress_bar = self._progress_bar_ctx.__enter__()
        self._progress_bar_total = total

    def _close_progress_bar(self):
        self._warn_if_v2_uses_v1_method("_close_progress_bar")
        if not self._progress_bar_ctx:
            return
        try:
            self._progress_bar_ctx.__exit__(None, None, None)
        except Exception as exc:
            BBLogger.log(f"Failed to close progress bar: {exc}")
        self._progress_bar_ctx = None
        self._progress_bar = None
        self._progress_bar_total = None

    def _init_v2(self, connection=None, config=None):
        self._subjective_api_version = "v2"
        self.name = ""
        self.session = None
        self.dependency_data_sources = []
        self.subscribers = []
        self.params = {}
        self._connection = dict(connection or {})
        self._config = dict(config or {})
        self.progress_callback = None
        self.status_callback = None
        self._progress_enabled = False
        self._progress_bar_ctx = None
        self._progress_bar = None
        self._progress_bar_total = None
        self._total_items = 0
        self._processed_items = 0
        self._total_processing_time = 0.0
        self._fetch_completed = False
        BBLogger._process_name = self._get_log_process_name()

    def _init_v1(self, *args, **kwargs):
        self._subjective_api_version = "v1"
        if args:
            values = list(args) + [None] * 5
            name, session, dependency_data_sources, subscribers, params = values[:5]
        else:
            name = kwargs.get("name")
            session = kwargs.get("session")
            dependency_data_sources = kwargs.get("dependency_data_sources")
            subscribers = kwargs.get("subscribers")
            params = kwargs.get("params")

        if (
            params is None
            and isinstance(session, dict)
            and dependency_data_sources is None
            and subscribers is None
        ):
            params = session
            session = None

        self.name = name
        self.session = session
        self.dependency_data_sources = list(dependency_data_sources or [])
        self.subscribers = list(subscribers or [])
        self.params = dict(params or {})
        self._connection = {}
        self._config = {}
        self._ensure_context_params()
        self.progress_callback = None
        self.status_callback = None
        self._progress_enabled = False
        self._progress_bar_ctx = None
        self._progress_bar = None
        self._progress_bar_total = None
        self._total_items = 0
        self._processed_items = 0
        self._total_processing_time = 0.0
        self._fetch_completed = False
        self._configure_progress_from_params()
        BBLogger._process_name = self._get_log_process_name()

    @classmethod
    def _extract_v1_metadata(cls, ds_class=None):
        ds_class = ds_class or cls
        try:
            bare = ds_class.__new__(ds_class)
            bare.params = {}
            bare.name = ds_class.__name__
            bare.session = None
            bare.subscribers = []
            bare.dependency_data_sources = []
            bare._connection = {}
            bare._config = {}
            bare.progress_callback = None
            bare.status_callback = None
            bare._progress_enabled = False
            bare._progress_bar_ctx = None
            bare._progress_bar = None
            bare._progress_bar_total = None
            bare._total_items = 0
            bare._processed_items = 0
            bare._total_processing_time = 0.0
            bare._fetch_completed = False
            bare._subjective_api_version = "v1"
            return bare
        except Exception:
            pass
        try:
            return ds_class(params={})
        except Exception:
            return None

    @classmethod
    def _legacy_connection_definition(cls) -> dict:
        instance = cls._extract_v1_metadata()
        if instance is None:
            return {}
        try:
            data = instance.get_connection_data() or {}
            return data if isinstance(data, dict) else {}
        except Exception:
            try:
                fallback_instance = cls(params={})
                data = fallback_instance.get_connection_data() or {}
                return data if isinstance(data, dict) else {}
            except Exception:
                return {}

    @classmethod
    def _connection_definition_to_schema(cls, definition: dict) -> dict:
        if not isinstance(definition, dict):
            return {}
        if isinstance(definition.get("connection_form"), dict):
            return definition.get("connection_form") or {}

        schema = {}
        fields = definition.get("fields", [])
        if not isinstance(fields, list):
            return {}
        for field in fields:
            if isinstance(field, str):
                schema[field] = {
                    "type": "text",
                    "label": field.replace("_", " ").title(),
                }
                continue
            if not isinstance(field, dict):
                continue
            name = str(field.get("name") or "").strip()
            if not name:
                continue
            spec = dict(field)
            spec.pop("name", None)
            spec.setdefault("type", "text")
            spec.setdefault("label", name.replace("_", " ").title())
            schema[name] = spec
        return schema

    @staticmethod
    def _schema_to_field_list(schema: dict) -> list[dict]:
        if not isinstance(schema, dict):
            return []
        fields = []
        for name, spec in schema.items():
            if not isinstance(spec, dict):
                spec = {"type": "text", "label": str(name).replace("_", " ").title()}
            field = {"name": name}
            field.update(spec)
            fields.append(field)
        return fields
