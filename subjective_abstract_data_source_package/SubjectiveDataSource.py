from abc import ABC, abstractmethod
from brainboost_data_source_logger_package.BBLogger import BBLogger
from brainboost_configuration_package.BBConfig import BBConfig
import os
import json
from datetime import datetime
from collections.abc import Iterable


class SubjectiveDataSource(ABC):
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        original_fetch = cls.__dict__.get("fetch")
        if original_fetch is None:
            return

        def wrapped_fetch(self, *args, **kwargs):
            result = original_fetch(self, *args, **kwargs)
            self._write_context_output_from_fetch(result)
            return result

        cls.fetch = wrapped_fetch

    def __init__(self, name=None, session=None, dependency_data_sources=[], subscribers=None, params=None):
        self.name = name
        self.session = session
        self.dependency_data_sources = dependency_data_sources
        self.subscribers = subscribers or []
        self.params = params or {}
        self._ensure_context_params()
        self.progress_callback = None  # Initialize the progress callback to None
        self.status_callback = None    # Initialize the status callback to None
        self._progress_enabled = False
        self._progress_bar_ctx = None
        self._progress_bar = None
        self._progress_bar_total = None
        # progress variables normal to all datasources
        self._total_items = 0
        self._processed_items = 0
        self._total_processing_time = 0.0
        self._fetch_completed = False
        self._configure_progress_from_params()

        # Set unique BBLogger process name for this datasource
        BBLogger._process_name = self._get_log_process_name()

    def start(self):
        for ds in self.dependency_data_sources:
            ds.subscribe(self)
            ds.start()

    def update(self, data):
        # Treat each update as one processed item and emit progress updates.
        self.increment_processed_items()
        self._emit_progress()
        try:
            self._write_context_output(data)
        except Exception as e:
            BBLogger.log(f"Failed writing context output: {e}")
        for subscriber in self.subscribers:
            subscriber.notify(data)

    def subscribe(self, subscriber):
        if subscriber not in self.subscribers:
            self.subscribers.append(subscriber)
            BBLogger.log(f"Subscriber {subscriber} added.")

    def get_name(self):
        class_name = self.__class__.__name__
        if self.name:
            return f"{self.name}_{class_name}"
        return class_name

    def get_data_source_type_name(self):
        """
        Returns the name of the data source type by removing
        'Subjective' and 'DataSource' substrings from the class name.
        """
        class_name = self.__class__.__name__
        data_source_type_name = class_name.replace('Subjective', '').replace('RealTimeDataSource', '').replace('DataSource', '')
        return data_source_type_name

    def _sanitize_label(self, value):
        return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in value)

    def _get_connection_label(self):
        if not isinstance(self.params, dict):
            return None
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

    def _ensure_context_params(self):
        if not isinstance(self.params, dict):
            return
        context_value = None
        for key in ("context_dir", "TARGET_DIRECTORY", "target_directory", "CONTEXT_DIR"):
            value = self.params.get(key)
            if value:
                context_value = value
                break
        if not context_value:
            context_value = self._get_default_context_path()
        if not context_value:
            return
        for key in ("context_dir", "TARGET_DIRECTORY", "target_directory", "CONTEXT_DIR"):
            if not self.params.get(key):
                self.params[key] = context_value

    def _get_default_context_path(self):
        context_storage = self._safe_get_config_value("CONTEXT_STORAGE")
        if context_storage:
            return context_storage
        return os.path.join("com_subjective_userdata", "com_subjective_context")

    def _resolve_context_path(self):
        target_directory = self.params.get("TARGET_DIRECTORY")
        if target_directory:
            return target_directory
        target_directory = self.params.get("target_directory")
        if target_directory:
            return target_directory
        return self._get_default_context_path()

    def _write_context_output(self, data):
        context_dir = self._resolve_context_path()
        os.makedirs(context_dir, exist_ok=True)

        datasource_name = self.get_data_source_type_name()
        output_format = self._safe_get_config_value("CONTEXT_OUTPUT_FORMAT") or "json"
        name_convention = self._safe_get_config_value("CONTEXT_FILE_NAME_CONVENTION")
        if not name_convention:
            name_convention = "YYYY_MM_DD_HH_MM_SS-[ds_name]-context.${CONTEXT_OUTPUT_FORMAT}"
        connection_label = self._get_connection_label()
        if connection_label and "[connection_name]" not in name_convention:
            datasource_name = f"{datasource_name}-{connection_label}"
        ts = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        filename = name_convention.replace("YYYY_MM_DD_HH_MM_SS", ts)
        filename = filename.replace("[ds_name]", datasource_name)
        if "[connection_name]" in filename:
            filename = filename.replace("[connection_name]", connection_label or "unknown")
        filename = filename.replace("${CONTEXT_OUTPUT_FORMAT}", output_format)
        path = os.path.join(context_dir, filename)

        payload = data
        if not isinstance(payload, (dict, list)):
            payload = {"value": payload}

        with open(path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2, default=str)
        try:
            os.chmod(path, 0o666)
        except Exception:
            pass
        return path

    def _write_context_output_from_fetch(self, result):
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

    # === Abstract methods that must be implemented by each data source ===
    @abstractmethod
    def fetch(self):
        """Fetch data from the data source."""
        pass

    @abstractmethod
    def get_icon(self) -> str:
        """Return the SVG code for the data source icon."""
        pass

    @abstractmethod
    def get_connection_data(self) -> dict:
        """
        Return the connection type and required fields for this data source.
        This should return a dictionary with connection type and fields.
        Example:
        {
            "connection_type": "AWS",
            "fields": ["region", "access_key", "secret_key", "target_directory"]
        }
        """
        pass

    # === Progress Tracking Methods ===

    def set_progress_callback(self, callback):
        """
        Set a callback function to receive progress updates.
        The callback should accept four arguments:
        data_source_name, total_items, processed_items, estimated_time
        """
        self.progress_callback = callback
        BBLogger.log(f"Progress callback set to: {callback}")

    def set_status_callback(self, callback):
        """
        Set a callback function to receive status updates.
        The callback should accept two arguments:
        data_source_name, status
        """
        self.status_callback = callback
        BBLogger.log(f"Status callback set to: {callback}")

    def estimated_remaining_time(self):
        """
        Returns an estimate of the time required to process the remaining items.
        Calculated as:
          remaining_to_process() * average_time_per_item()
        """
        remaining_to_process = self.remaining_to_process()
        if remaining_to_process is None:
            BBLogger.log("Total items unknown; cannot estimate remaining time")
            return None
        remaining = remaining_to_process * self.average_time_per_item()
        BBLogger.log(f"Estimated remaining time: {remaining} seconds")
        return remaining

    def get_total_to_process(self):
        BBLogger.log(f"Getting total items to process: {self._total_items}")
        return self._total_items

    def get_total_processed(self):
        BBLogger.log(f"Getting total processed items: {self._processed_items}")
        return self._processed_items

    def get_total_processing_time(self):
        BBLogger.log(f"Getting total processing time: {self._total_processing_time} seconds")
        return self._total_processing_time

    def remaining_to_process(self):
        """
        Returns the remaining number of items to process.
        By default, this is calculated as:
          total_to_process() - total_processed()
        """
        total = self.get_total_to_process()
        if total <= 0:
            BBLogger.log("Total items unknown; remaining items cannot be computed")
            return None
        remaining = total - self.get_total_processed()
        BBLogger.log(f"Remaining items to process: {remaining}")
        return remaining

    def increment_processed_items(self):
        new_processed = self.get_total_processed() + 1
        self.set_processed_items(new_processed)
        BBLogger.log(f"Incremented processed items to: {new_processed}")

    def set_total_items(self, total_items):
        self._total_items = total_items
        BBLogger.log(f"Set total items to: {total_items}")
        if self._progress_enabled:
            self._ensure_progress_bar(restart_if_unknown=True)

    def set_processed_items(self, processed_items):
        self._processed_items = processed_items
        BBLogger.log(f"Set processed items to: {processed_items}")

    def set_total_processing_time(self, total_processing_time):
        self._total_processing_time = total_processing_time
        BBLogger.log(f"Set total processing time to: {total_processing_time} seconds")

    def set_fetch_completed(self, fetch_completed=False):
        self._fetch_completed = fetch_completed
        BBLogger.log(f"Set fetch completed to: {fetch_completed}")
        if fetch_completed:
            self._close_progress_bar()

    def average_time_per_item(self):
        if self.get_total_processed() == 0:
            BBLogger.log("No items processed yet; average time per item is 0.0 seconds")
            return 0.0
        avg = self.get_total_processing_time() / self.get_total_processed()
        BBLogger.log(f"Calculated average time per item: {avg} seconds")
        return avg

    def _emit_progress(self):
        if self._progress_enabled:
            self._ensure_progress_bar()
            if self._progress_bar:
                try:
                    self._progress_bar()
                except Exception as e:
                    BBLogger.log(f"Progress bar update failed: {e}")
        total_items = self.get_total_to_process()
        processed_items = self.get_total_processed()
        estimated_time = None if total_items <= 0 else self.estimated_remaining_time()
        if self.progress_callback:
            try:
                self.progress_callback(self.get_name(), total_items, processed_items, estimated_time)
            except Exception as e:
                BBLogger.log(f"Progress callback failed: {e}")

    def enable_progress_bar(self, enabled=True):
        self._progress_enabled = bool(enabled)
        if self._progress_enabled:
            self._ensure_progress_bar()
        else:
            self._close_progress_bar()

    def _configure_progress_from_params(self):
        progress_flag = False
        if isinstance(self.params, dict):
            progress_flag = bool(self.params.get("progress") or self.params.get("--progress"))
        elif isinstance(self.params, (list, tuple, set)):
            progress_flag = "--progress" in self.params or "progress" in self.params
        if progress_flag:
            self.enable_progress_bar(True)

    def _ensure_progress_bar(self, restart_if_unknown=False):
        if not self._progress_enabled:
            return
        try:
            from alive_progress import alive_bar
        except Exception as e:
            BBLogger.log(f"alive-progress not available: {e}")
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
        if not self._progress_bar_ctx:
            return
        try:
            self._progress_bar_ctx.__exit__(None, None, None)
        except Exception as e:
            BBLogger.log(f"Failed to close progress bar: {e}")
        self._progress_bar_ctx = None
        self._progress_bar = None
        self._progress_bar_total = None
