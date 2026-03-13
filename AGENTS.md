# AGENTS.md

## Purpose

This repository defines the base contracts for BrainBoost data sources. When an LLM agent is asked to create or update a data source, treat this file as the implementation contract.

## Choose the Right Base Class

- Use `SubjectiveDataSource` for batch, polling, one-shot, or processor-style data sources.
- Use `SubjectiveRealTimeDataSource` for long-running monitors, listeners, sockets, watchers, or streaming integrations.
- Use `SubjectiveOnDemandDataSource` for request/response, chat-like, query-based, or tool-invocation data sources.
- Use `SubjectivePipelineDataSource` only when the task is to wrap a JSON pipeline as a data source. Do not use it for ordinary plugins.

## Class and File Naming

- Prefer class names in the form `SubjectiveSomethingDataSource`.
- If the class must be discoverable by the shared importer, keep it in a file ending with `DataSource.py`.
- Keep the exported class name stable. Pipeline JSON and plugin discovery depend on class names.
- Prefer one primary data source class per file.

## Constructor Contract

- Support this signature unless there is a strong reason not to:

```python
def __init__(
    self,
    name=None,
    session=None,
    dependency_data_sources=None,
    subscribers=None,
    params=None,
):
```

- Always call `super().__init__(...)` with named arguments.
- Treat `params` as the main configuration input.
- Do not require secrets, paths, or connection settings as positional arguments.
- Read optional configuration from `self.params`.

## Required Methods

### `SubjectiveDataSource`

- Implement `fetch(self)`.
- Implement `get_icon(self) -> str`.
- Implement `get_connection_data(self) -> dict`.

### `SubjectiveRealTimeDataSource`

- Implement `get_icon(self) -> str`.
- Implement `get_connection_data(self) -> dict`.
- Override monitoring hooks when needed:
  - `_initialize_monitoring()`
  - `_start_monitoring_implementation()`
  - `_stop_monitoring_implementation()`
- For reconnecting streams, prefer `_connect_stream()`, `_run_stream()`, and optional `_disconnect_stream()`.

### `SubjectiveOnDemandDataSource`

- Implement `_process_message(self, message)`.
- Implement `get_icon(self) -> str`.
- Implement `get_connection_data(self) -> dict`.
- Let the base class own async queueing, callbacks, history, and message dispatch.

## Data Flow Rules

- Prefer JSON-serializable payloads: `dict`, `list`, `str`, numbers, booleans.
- Use `self.update(data)` when the datasource emits data to subscribers.
- `SubjectiveDataSource.update()` also writes context output files.
- `SubjectiveDataSource.fetch()` is wrapped by `__init_subclass__`; any value returned from `fetch()` is also written to context output.
- Avoid accidental duplicates: if `fetch()` already calls `self.update()` for a payload, do not also return the same payload unless duplicate context files are acceptable.
- If the datasource processes multiple items, emit them individually with `self.update(...)`.

## Pipeline Compatibility

- If the datasource may be used as a downstream node in `SubjectiveDataSourcePipeline`, implement:

```python
def process_input(self, data):
    result = {"input": data}
    self.update(result)
```

- `SubjectiveDataSourcePipeline` prefers `process_input(data)` when present.
- If `process_input` is absent, the pipeline may fall back to `update(data)`, which is usually not enough for processor nodes.
- Keep dependency-driven logic inside `process_input`, not inside `fetch()`.

## Connection Metadata

- `get_connection_data()` must return a dictionary containing `connection_type` and `fields`.
- Prefer field definitions as dictionaries, not bare strings.
- If the datasource represents a named external connection, expose and honor `connection_name` in `params`. It is used by logging, temp-dir naming, and some pipeline routing flows.
- Recommended field shape:

```python
{
    "connection_type": "MY_TYPE",
    "fields": [
        {
            "name": "api_key",
            "type": "password",
            "label": "API Key",
            "description": "Credential for the remote service",
            "required": True,
        }
    ],
}
```

- Use clear, UI-friendly field names and labels.
- Keep field names aligned with keys expected in `self.params`.

## Context Output and Temporary Files

- The base class auto-populates context directory params when possible.
- Do not hardcode context output paths.
- Prefer `context_dir` or related keys in `params` only when the task explicitly requires it.
- For scratch or temporary files, use `self.get_connection_temp_dir()`.
- Do not write outside the configured workspace unless the user explicitly asks.

## Progress Reporting

- If the datasource knows the workload size, call `self.set_total_items(total)`.
- After each processed item, call `self.update(...)` or manually update processed counts if the flow requires it.
- If you track elapsed work, update `self.set_total_processing_time(...)`.
- Call `self.set_fetch_completed(True)` when a finite batch fetch is done.

## Real-Time Data Source Rules

- Use `send_notification(...)` or `self.update(...)` to emit events.
- Ensure monitoring can be stopped cleanly.
- Release sockets, watchers, threads, and other resources in stop paths.
- Respect reconnect settings from `self.params` when using stream reconnection:
  - `reconnect_initial_delay`
  - `reconnect_max_delay`
  - `reconnect_jitter`
  - `reconnect_max_attempts`

## On-Demand Data Source Rules

- Put business logic inside `_process_message`.
- Return structured responses when possible.
- For recoverable failures, prefer structured error payloads.
- If file attachments matter, use the base class attachment flow instead of inventing a new payload format.
- Test both sync mode (`async_mode=False`) and async mode when the datasource supports both.

## Logging and Error Handling

- Use `BBLogger.log(...)` for operational logging.
- Raise clear exceptions for missing required config.
- Do not silently swallow important failures.
- If you convert exceptions into response payloads, preserve enough detail to debug the issue safely.

## Import and Discovery Rules

- Shared importer search order is:
  1. `data_source_addons.<ClassName>`
  2. installed plugin directories containing files ending in `DataSource.py`
  3. `subjective_abstract_data_source_package`
- If the datasource is meant for pipeline JSON loading, keep the class name unique and consistent.
- If the task is to create a production plugin, prefer placing it in a plugin repo or plugin directory rather than inside this abstract base package.

## Minimal Implementation Template

```python
from subjective_abstract_data_source_package import SubjectiveDataSource


class SubjectiveExampleDataSource(SubjectiveDataSource):
    def __init__(self, name=None, session=None, dependency_data_sources=None, subscribers=None, params=None):
        super().__init__(
            name=name,
            session=session,
            dependency_data_sources=dependency_data_sources or [],
            subscribers=subscribers,
            params=params or {},
        )
        self.endpoint = self.params.get("endpoint")

    def fetch(self):
        if not self.endpoint:
            raise ValueError("Missing required param: endpoint")
        result = {"status": "ok", "endpoint": self.endpoint}
        self.update(result)

    def process_input(self, data):
        transformed = {"input": data, "endpoint": self.endpoint}
        self.update(transformed)

    def get_icon(self) -> str:
        return "<svg viewBox='0 0 24 24'><circle cx='12' cy='12' r='10'/></svg>"

    def get_connection_data(self) -> dict:
        return {
            "connection_type": "EXAMPLE",
            "fields": [
                {
                    "name": "endpoint",
                    "type": "text",
                    "label": "Endpoint",
                    "required": True,
                }
            ],
        }
```

## Testing Expectations

- Add or update `pytest` tests for every new datasource.
- Minimum coverage should include:
  - constructor/config behavior
  - happy-path fetch or message processing
  - subscriber notification via `update(...)`
  - error handling
  - `get_connection_data()` structure
  - `process_input()` if the datasource is pipeline-capable
- For on-demand datasources, test sync and async behavior.
- For real-time datasources, test lifecycle and emitted notifications without depending on live external systems when possible.

## Definition of Done

- The correct base class is used.
- Required abstract methods are implemented.
- Configuration is driven through `params`.
- Context output is not duplicated unintentionally.
- Pipeline compatibility has been considered.
- Tests exist for the main execution path and failure path.
- Public-facing configuration metadata is present and usable.
