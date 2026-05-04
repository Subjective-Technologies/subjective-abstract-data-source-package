from __future__ import annotations

import sys
import types
import warnings
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


if "brainboost_configuration_package.BBConfig" not in sys.modules:
    config_pkg = types.ModuleType("brainboost_configuration_package")
    config_mod = types.ModuleType("brainboost_configuration_package.BBConfig")

    class BBConfig:
        @staticmethod
        def get(_key, default=None):
            if default is not None:
                return default
            raise KeyError(_key)

    config_mod.BBConfig = BBConfig
    config_pkg.BBConfig = BBConfig
    sys.modules["brainboost_configuration_package"] = config_pkg
    sys.modules["brainboost_configuration_package.BBConfig"] = config_mod


if "brainboost_data_source_logger_package.BBLogger" not in sys.modules:
    logger_pkg = types.ModuleType("brainboost_data_source_logger_package")
    logger_mod = types.ModuleType("brainboost_data_source_logger_package.BBLogger")

    class BBLogger:
        _process_name = ""

        @staticmethod
        def log(_message):
            return None

    logger_mod.BBLogger = BBLogger
    logger_pkg.BBLogger = BBLogger
    sys.modules["brainboost_data_source_logger_package"] = logger_pkg
    sys.modules["brainboost_data_source_logger_package.BBLogger"] = logger_mod


from subjective_abstract_data_source_package.SubjectiveDataSource import SubjectiveDataSource
from subjective_abstract_data_source_package.SubjectiveOnDemandDataSource import (
    SubjectiveOnDemandDataSource,
)
from subjective_abstract_data_source_package.SubjectiveRealTimeDataSource import (
    SubjectiveRealTimeDataSource,
)


class BasicV2DataSource(SubjectiveDataSource):
    @classmethod
    def connection_schema(cls):
        return {"token": {"type": "password", "label": "Token"}}

    def run(self, request):
        return {"request": request}


class BasicV1DataSource(SubjectiveDataSource):
    def fetch(self):
        return {"ok": True}

    def get_icon(self) -> str:
        return "<svg></svg>"

    def get_connection_data(self) -> dict:
        return {
            "connection_type": "LEGACY",
            "fields": [{"name": "endpoint", "type": "text"}],
        }


def test_v2_class_missing_run_raises_typeerror():
    with pytest.raises(TypeError, match="must implement run"):

        class MissingRunDataSource(SubjectiveDataSource):
            @classmethod
            def connection_schema(cls):
                return {}


def test_v2_class_missing_connection_schema_raises_typeerror():
    with pytest.raises(TypeError, match="must implement connection_schema"):

        class MissingConnectionSchemaDataSource(SubjectiveDataSource):
            def run(self, request):
                return request


def test_v2_class_with_both_methods_succeeds():
    class CompleteV2DataSource(SubjectiveDataSource):
        @classmethod
        def connection_schema(cls):
            return {"api_key": {"type": "password"}}

        def run(self, request):
            return request

    assert CompleteV2DataSource.api_version() == "v2"


def test_v1_class_without_run_succeeds():
    class CompleteV1DataSource(SubjectiveDataSource):
        def fetch(self):
            return {"ok": True}

        def get_icon(self) -> str:
            return "<svg></svg>"

        def get_connection_data(self) -> dict:
            return {"connection_type": "LEGACY", "fields": ["endpoint"]}

    assert CompleteV1DataSource.api_version() == "v1"


def test_wrapper_classes_are_v1():
    assert SubjectiveOnDemandDataSource.api_version() == "v1"
    assert SubjectiveRealTimeDataSource.api_version() == "v1"


def test_v2_init_with_connection_kwarg():
    instance = BasicV2DataSource(connection={"key": "val"})

    assert instance._connection == {"key": "val"}
    assert instance._subjective_api_version == "v2"


def test_v2_init_with_connection_and_config_kwargs():
    instance = BasicV2DataSource(
        connection={"key": "val"},
        config={"output_dir": "/tmp"},
    )

    assert instance._connection == {"key": "val"}
    assert instance._config["output_dir"] == "/tmp"
    assert instance._subjective_api_version == "v2"


def test_v1_init_with_name_kwarg(monkeypatch):
    monkeypatch.setattr(SubjectiveDataSource, "_ensure_context_params", lambda self: None)

    instance = BasicV1DataSource(name="test", params={"key": "val"})

    assert instance.name == "test"
    assert instance.params["key"] == "val"
    assert instance._subjective_api_version == "v1"


def test_v1_init_with_positional_args(monkeypatch):
    monkeypatch.setattr(SubjectiveDataSource, "_ensure_context_params", lambda self: None)

    instance = BasicV1DataSource("name", None, [], None, {"k": "v"})

    assert instance.name == "name"
    assert instance.params == {"k": "v"}
    assert instance._subjective_api_version == "v1"


def test_v1_init_with_params_only(monkeypatch):
    monkeypatch.setattr(SubjectiveDataSource, "_ensure_context_params", lambda self: None)

    instance = BasicV1DataSource(params={"k": "v"})

    assert instance.params == {"k": "v"}
    assert instance._subjective_api_version == "v1"


def test_v2_instance_warns_on_subscribe():
    instance = BasicV2DataSource(connection={}, config={})

    with pytest.warns(DeprecationWarning, match="v1 method"):
        instance.subscribe(object())


def test_v2_instance_warns_on_update():
    instance = BasicV2DataSource(connection={}, config={})

    with pytest.warns(DeprecationWarning, match="v1 method"):
        instance.update({})


def test_v1_instance_no_warning_on_subscribe():
    instance = BasicV1DataSource(params={})

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        instance.subscribe(object())

    assert not any(item.category is DeprecationWarning for item in caught)


def test_realtime_without_stream_impl_returns_false():
    class BareRealtimeDataSource(SubjectiveRealTimeDataSource):
        def get_icon(self) -> str:
            return ""

        def get_connection_data(self) -> dict:
            return {}

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        instance = BareRealtimeDataSource(params={})

    assert instance.supports_streaming() is False


def test_realtime_with_connect_run_stream_returns_true():
    class ConnectRunRealtimeDataSource(SubjectiveRealTimeDataSource):
        def get_icon(self) -> str:
            return ""

        def get_connection_data(self) -> dict:
            return {}

        def _connect_stream(self):
            return None

        def _run_stream(self):
            return None

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        instance = ConnectRunRealtimeDataSource(params={})

    assert instance.supports_streaming() is True


def test_realtime_with_stream_override_returns_true():
    class StreamRealtimeDataSource(SubjectiveRealTimeDataSource):
        def get_icon(self) -> str:
            return ""

        def get_connection_data(self) -> dict:
            return {}

        def stream(self, request: dict):
            yield request

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        instance = StreamRealtimeDataSource(params={})

    assert instance.supports_streaming() is True


def test_realtime_with_monitoring_impl_returns_true():
    class MonitoringRealtimeDataSource(SubjectiveRealTimeDataSource):
        def get_icon(self) -> str:
            return ""

        def get_connection_data(self) -> dict:
            return {}

        def _start_monitoring_implementation(self):
            return None

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        instance = MonitoringRealtimeDataSource(params={})

    assert instance.supports_streaming() is True


def test_extract_v1_metadata_no_init_side_effects():
    init_calls = []

    class SideEffectV1DataSource(SubjectiveDataSource):
        def __init__(self, *args, **kwargs):
            init_calls.append((args, kwargs))

        def fetch(self):
            return {"ok": True}

        def get_icon(self) -> str:
            return "<svg></svg>"

        def get_connection_data(self) -> dict:
            return {"connection_type": "TEST", "fields": ["endpoint"]}

    metadata = SubjectiveDataSource._extract_v1_metadata(SideEffectV1DataSource)

    assert metadata is not None
    assert init_calls == []


def test_extract_v1_metadata_returns_instance_with_params():
    class MetadataV1DataSource(SubjectiveDataSource):
        def fetch(self):
            return {"ok": True}

        def get_icon(self) -> str:
            return "<svg></svg>"

        def get_connection_data(self) -> dict:
            return {"connection_type": "TEST", "fields": ["endpoint"]}

    metadata = SubjectiveDataSource._extract_v1_metadata(MetadataV1DataSource)

    assert metadata is not None
    assert metadata.params == {}
    assert metadata.name == "MetadataV1DataSource"


def test_v2_connection_schema_returns_declared_fields():
    class SchemaV2DataSource(SubjectiveDataSource):
        @classmethod
        def connection_schema(cls):
            return {
                "api_key": {"type": "password"},
                "endpoint": {"type": "text"},
            }

        def run(self, request):
            return request

    assert SchemaV2DataSource.connection_schema() == {
        "api_key": {"type": "password"},
        "endpoint": {"type": "text"},
    }


def test_v2_request_schema_defaults_empty():
    class DefaultRequestSchemaV2DataSource(SubjectiveDataSource):
        @classmethod
        def connection_schema(cls):
            return {"token": {"type": "password"}}

        def run(self, request):
            return request

    assert DefaultRequestSchemaV2DataSource.request_schema() == {}


def test_v2_output_schema_defaults_empty():
    class DefaultOutputSchemaV2DataSource(SubjectiveDataSource):
        @classmethod
        def connection_schema(cls):
            return {"token": {"type": "password"}}

        def run(self, request):
            return request

    assert DefaultOutputSchemaV2DataSource.output_schema() == {}


def test_v2_actions_auto_generate_request_and_output_schema():
    class ActionBackedV2DataSource(SubjectiveDataSource):
        @classmethod
        def connection_schema(cls):
            return {"token": {"type": "password"}}

        @classmethod
        def actions(cls):
            return {
                "Create": {
                    "request": {
                        "prompt": {"type": "text"},
                    },
                    "output": {
                        "task_id": {"type": "text"},
                    },
                },
                "Status": {
                    "request": {
                        "task_id": {"type": "text"},
                    },
                    "output": {
                        "status": {"type": "text"},
                    },
                },
            }

        def run(self, request):
            return request

    assert ActionBackedV2DataSource.request_schema() == {
        "prompt": {"type": "text"},
        "task_id": {"type": "text"},
    }
    assert ActionBackedV2DataSource.output_schema() == {
        "task_id": {"type": "text"},
        "status": {"type": "text"},
    }


def test_v2_action_schema_returns_specific_action():
    class ActionSchemaV2DataSource(SubjectiveDataSource):
        @classmethod
        def connection_schema(cls):
            return {}

        @classmethod
        def actions(cls):
            return {
                "Create": {
                    "request": {"prompt": {"type": "text"}},
                    "output": {"task_id": {"type": "text"}},
                }
            }

        def run(self, request):
            return request

    assert ActionSchemaV2DataSource.action_schema("Create") == {
        "request": {"prompt": {"type": "text"}},
        "output": {"task_id": {"type": "text"}},
    }
    assert ActionSchemaV2DataSource.action_schema("Missing") == {}


def test_v2_run_called_directly():
    class RunEchoV2DataSource(SubjectiveDataSource):
        @classmethod
        def connection_schema(cls):
            return {"token": {"type": "password"}}

        def run(self, request):
            return {"result": request["q"]}

    instance = RunEchoV2DataSource(connection={}, config={})

    assert instance.run({"q": "hello"}) == {"result": "hello"}


def test_v1_run_delegates_to_fetch(monkeypatch):
    monkeypatch.setattr(SubjectiveDataSource, "_ensure_context_params", lambda self: None)

    class RunDelegatingV1DataSource(SubjectiveDataSource):
        def fetch(self):
            return {"data": 1}

        def get_icon(self) -> str:
            return "<svg></svg>"

        def get_connection_data(self) -> dict:
            return {"connection_type": "TEST", "fields": ["endpoint"]}

    instance = RunDelegatingV1DataSource(params={})

    assert instance.run({}) == {"data": 1}


def test_send_notification_adds_timestamp_and_delegates_to_update(monkeypatch):
    monkeypatch.setattr(SubjectiveDataSource, "_ensure_context_params", lambda self: None)
    instance = BasicV1DataSource(params={})
    payload = {"event": "created"}
    captured = []

    def fake_update(data):
        captured.append(dict(data))

    monkeypatch.setattr(instance, "update", fake_update)

    instance.send_notification(payload)

    assert "timestamp" in payload
    assert captured == [payload]


def test_send_redis_notification_gracefully_handles_missing_redis(monkeypatch):
    monkeypatch.setitem(sys.modules, "redis", None)
    instance = BasicV2DataSource(connection={}, config={})

    instance.send_redis_notification("test_channel", {"event": "test"})


def test_send_redis_notification_publishes_serialized_payload(monkeypatch):
    published = {}

    class FakeRedisClient:
        def publish(self, channel, payload):
            published["channel"] = channel
            published["payload"] = payload

    class FakeRedisModule:
        @staticmethod
        def Redis(host, port, db):
            published["host"] = host
            published["port"] = port
            published["db"] = db
            return FakeRedisClient()

    monkeypatch.setitem(sys.modules, "redis", FakeRedisModule())
    instance = BasicV2DataSource(connection={}, config={})

    instance.send_redis_notification("test_channel", {"event": "test"})

    assert published["channel"] == "test_channel"
    assert published["host"] == "localhost"
    assert published["port"] == 6379
    assert published["db"] == 0
    assert '"event": "test"' in published["payload"]
    assert '"timestamp"' in published["payload"]
