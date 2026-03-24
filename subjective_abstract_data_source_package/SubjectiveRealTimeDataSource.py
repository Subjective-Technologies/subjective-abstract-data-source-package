from __future__ import annotations

import threading
import time
import warnings

from brainboost_data_source_logger_package.BBLogger import BBLogger

from .SubjectiveDataSource import SubjectiveDataSource


class SubjectiveRealTimeDataSource(SubjectiveDataSource):
    """
    Deprecated compatibility wrapper for legacy real-time datasources.

    New datasources should inherit from SubjectiveDataSource directly and implement:
      - supports_streaming()
      - stream(request)
    """

    _subjective_api_version = "v1"

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "SubjectiveRealTimeDataSource is deprecated. "
            "Use SubjectiveDataSource with supports_streaming()/stream().",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
        self._monitoring_active = False
        self._monitoring_thread = None
        self._reconnect_enabled = True
        self._reconnect_initial_delay = self._coerce_float_param("reconnect_initial_delay", 1.0)
        self._reconnect_max_delay = self._coerce_float_param("reconnect_max_delay", 30.0)
        self._reconnect_jitter = self._coerce_float_param("reconnect_jitter", 0.2)
        self._reconnect_max_attempts = self._coerce_int_param("reconnect_max_attempts", -1)

    def supports_streaming(self) -> bool:
        """Returns True only if this subclass can actually stream."""
        if "stream" in type(self).__dict__ and type(self).__dict__["stream"] is not SubjectiveRealTimeDataSource.stream:
            return True
        if (
            hasattr(self, "_connect_stream")
            and callable(getattr(self, "_connect_stream"))
            and hasattr(self, "_run_stream")
            and callable(getattr(self, "_run_stream"))
        ):
            return True
        if (
            "_start_monitoring_implementation" in type(self).__dict__
            and type(self).__dict__["_start_monitoring_implementation"]
            is not SubjectiveRealTimeDataSource._start_monitoring_implementation
        ):
            return True
        return False

    def stream(self, request: dict):
        raise NotImplementedError("Streaming not supported by this datasource")

    def _coerce_float_param(self, key, default):
        try:
            value = {}
            if isinstance(self.params, dict):
                value = self.params
            elif isinstance(self._config, dict):
                value = self._config
            return float(value.get(key, default))
        except (AttributeError, TypeError, ValueError):
            return float(default)

    def _coerce_int_param(self, key, default):
        try:
            value = {}
            if isinstance(self.params, dict):
                value = self.params
            elif isinstance(self._config, dict):
                value = self._config
            return int(value.get(key, default))
        except (AttributeError, TypeError, ValueError):
            return int(default)

    def _get_reconnect_delay(self, attempt):
        base_delay = min(
            self._reconnect_initial_delay * (2 ** max(attempt - 1, 0)),
            self._reconnect_max_delay,
        )
        jitter = base_delay * self._reconnect_jitter
        return max(0.0, base_delay + (jitter * (0.5 - time.time() % 1)))

    def _should_retry(self, attempt):
        if not self._reconnect_enabled:
            return False
        if self._reconnect_max_attempts < 0:
            return True
        return attempt <= self._reconnect_max_attempts

    def _run_with_reconnect(self, connect_fn, run_fn, disconnect_fn=None, label="real-time stream"):
        attempt = 0
        while self._monitoring_active:
            attempt += 1
            try:
                BBLogger.log(f"{label} - starting (attempt {attempt})")
                if callable(connect_fn):
                    connect_fn()
                run_fn()
                if not self._monitoring_active:
                    break
                BBLogger.log(f"{label} - ended; preparing to reconnect")
            except Exception as exc:
                if not self._monitoring_active:
                    break
                BBLogger.log(f"{label} - error: {exc}")
            finally:
                if callable(disconnect_fn):
                    try:
                        disconnect_fn()
                    except Exception as cleanup_err:
                        BBLogger.log(f"{label} - cleanup error: {cleanup_err}")

            if not self._should_retry(attempt):
                BBLogger.log(f"{label} - reconnect disabled or max attempts reached")
                break

            delay = self._get_reconnect_delay(attempt)
            if delay > 0:
                BBLogger.log(f"{label} - reconnecting in {delay:.2f}s")
                time.sleep(delay)

    def start_monitoring(self):
        if self._monitoring_active:
            BBLogger.log("Monitoring is already active")
            return

        BBLogger.log(f"Starting monitoring for {self.__class__.__name__}")
        self._monitoring_active = True

        try:
            self._initialize_monitoring()
            self._monitoring_thread = threading.Thread(
                target=self._start_monitoring_implementation,
                daemon=True,
                name=f"{self.__class__.__name__}_monitor",
            )
            self._monitoring_thread.start()
        except Exception:
            self._monitoring_active = False
            raise

    def stop_monitoring(self):
        if not self._monitoring_active:
            return

        BBLogger.log(f"Stopping monitoring for {self.__class__.__name__}")
        self._monitoring_active = False
        try:
            self._stop_monitoring_implementation()
        except Exception as exc:
            BBLogger.log(f"Error while stopping monitoring: {exc}")

    def _initialize_monitoring(self):
        pass

    def _start_monitoring_implementation(self):
        has_connect = hasattr(self, "_connect_stream") and callable(getattr(self, "_connect_stream"))
        has_run = hasattr(self, "_run_stream") and callable(getattr(self, "_run_stream"))

        if has_connect and has_run:
            disconnect_fn = getattr(self, "_disconnect_stream", None)
            label = getattr(self, "_stream_label", f"{self.__class__.__name__} stream")
            self._run_with_reconnect(
                connect_fn=getattr(self, "_connect_stream"),
                run_fn=getattr(self, "_run_stream"),
                disconnect_fn=disconnect_fn,
                label=label,
            )
            return

        for item in self.stream({}):
            if not self._monitoring_active:
                break
            self.update(item)

    def _stop_monitoring_implementation(self):
        pass

    def stop(self):
        self.stop_monitoring()
        if self._monitoring_thread and self._monitoring_thread.is_alive():
            self._monitoring_thread.join(timeout=5.0)

    def fetch(self):
        BBLogger.log(f"Fetch called on real-time data source {self.__class__.__name__}")
        self.start_monitoring()
        if self._monitoring_thread and self._monitoring_thread.is_alive():
            try:
                self._monitoring_thread.join()
            except KeyboardInterrupt:
                BBLogger.log("Fetch interrupted, stopping monitoring")
                self.stop_monitoring()
