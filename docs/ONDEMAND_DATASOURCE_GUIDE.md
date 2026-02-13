# OnDemand DataSource Implementation Guide

## Overview

`SubjectiveOnDemandDataSource` is an abstract base class for creating request/response style data sources. These work like a conversation or chat interface where messages are sent and responses are received.

Use this base class when you need to implement:
- Chat/conversation interfaces
- API request/response wrappers
- Query-based data sources
- Any interactive request/response pattern

## Required Methods to Implement

When creating a new OnDemand data source, you **MUST** implement these three abstract methods:

### 1. `_process_message(self, message: Any) -> Any`

This is the core method that handles incoming messages and returns responses.

```python
@abstractmethod
def _process_message(self, message: Any) -> Any:
    """
    Process an incoming message and return a response.

    Args:
        message: The incoming message (string, dict, or any object)

    Returns:
        The response to send back to the caller
    """
    pass
```

### 2. `get_icon(self) -> str`

Returns an SVG string representing the data source icon for UI display.

```python
@abstractmethod
def get_icon(self) -> str:
    """Return the SVG code for the data source icon."""
    pass
```

### 3. `get_connection_data(self) -> dict`

Returns connection metadata describing required configuration fields.

```python
@abstractmethod
def get_connection_data(self) -> dict:
    """
    Return the connection type and required fields.

    Returns:
        Dictionary with connection_type and fields list
    """
    pass
```

## Complete Implementation Template

```python
from subjective_abstract_data_source_package import SubjectiveOnDemandDataSource
from typing import Any


class MyOnDemandDataSource(SubjectiveOnDemandDataSource):
    """
    Description of what this data source does.
    """

    def __init__(self, name=None, session=None, dependency_data_sources=None,
                 subscribers=None, params=None):
        super().__init__(
            name=name,
            session=session,
            dependency_data_sources=dependency_data_sources,
            subscribers=subscribers,
            params=params
        )
        # Initialize any custom attributes here
        self.api_key = self.params.get("api_key")
        self.endpoint = self.params.get("endpoint")

    def _process_message(self, message: Any) -> Any:
        """
        Process the incoming message and return a response.

        Args:
            message: The message to process

        Returns:
            The response data
        """
        # Your processing logic here
        # Example: call an API, process data, etc.

        response = {
            "input": message,
            "output": f"Processed: {message}",
            "status": "success"
        }
        return response

    def get_icon(self) -> str:
        """Return SVG icon for this data source."""
        return '''<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2z"
                  fill="#4F46E5"/>
            <path d="M8 12l2 2 4-4" stroke="white" stroke-width="2"
                  stroke-linecap="round" stroke-linejoin="round"/>
        </svg>'''

    def get_connection_data(self) -> dict:
        """Return connection configuration metadata."""
        return {
            "connection_type": "ON_DEMAND",
            "fields": [
                {
                    "name": "api_key",
                    "type": "password",
                    "label": "API Key",
                    "required": True
                },
                {
                    "name": "endpoint",
                    "type": "text",
                    "label": "API Endpoint",
                    "required": True
                }
            ]
        }
```

## Usage Examples

### Basic Synchronous Usage

```python
# Create instance with sync mode
datasource = MyOnDemandDataSource(
    name="my_datasource",
    params={
        "async_mode": False,  # Disable async for blocking calls
        "api_key": "your-api-key",
        "endpoint": "https://api.example.com"
    }
)

# Send message and get response directly
response = datasource.send_message("Hello how are you today")
print(response)
```

### Asynchronous Usage with Callback

```python
def handle_response(response):
    print(f"Received response: {response}")

# Create instance (async mode is default)
datasource = MyOnDemandDataSource(
    name="my_datasource",
    params={
        "api_key": "your-api-key",
        "endpoint": "https://api.example.com"
    }
)

# Set response callback
datasource.set_response_callback(handle_response)

# Send messages (non-blocking)
datasource.send_message("Hello how are you today")
datasource.send_message("What is the weather?")

# Wait for all messages to be processed
datasource.wait_for_pending()

# Stop processing when done
datasource.stop()
```

### Using in a Pipeline

```python
# OnDemand data sources can be used in pipelines
# The fetch() method starts the processing loop

datasource = MyOnDemandDataSource(params={...})
datasource.fetch()  # Starts async processing loop

# Now send messages
datasource.send_message("Process this data")
```

### Accessing Conversation History

```python
datasource = MyOnDemandDataSource(params={"async_mode": False})

datasource.send_message("First message")
datasource.send_message("Second message")

# Get conversation history
history = datasource.get_conversation_history()
for entry in history:
    print(f"[{entry['role']}]: {entry['content']}")

# Clear history if needed
datasource.clear_conversation_history()
```

## Configuration Parameters

These parameters can be passed in the `params` dictionary:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `async_mode` | bool | `True` | Enable async message processing |
| `max_history` | int | `100` | Maximum conversation history entries |
| `context_dir` | str | auto | Directory for context output files |
| `progress` | bool | `False` | Enable progress bar |

## Context Output

Each processed message generates a JSON context file in the context directory:

**Filename format:** `YYYY_MM_DD_HH_MM_SS-OnDemand-context.json`

**Content structure:**
```json
{
  "request": "Hello how are you today",
  "response": {
    "input": "Hello how are you today",
    "output": "Processed: Hello how are you today",
    "status": "success"
  },
  "processing_time": 0.15,
  "timestamp": 1737120645.123
}
```

## Available Methods

### Core Methods

| Method | Description |
|--------|-------------|
| `send_message(message)` | Send a message and get response |
| `set_response_callback(callback)` | Set async response handler |
| `fetch()` | Start processing loop (for pipeline usage) |
| `stop()` | Stop the processing loop |
| `wait_for_pending(timeout)` | Wait for queued messages |

### Conversation History

| Method | Description |
|--------|-------------|
| `get_conversation_history()` | Get list of all messages/responses |
| `clear_conversation_history()` | Clear the history |

### Inherited from SubjectiveDataSource

| Method | Description |
|--------|-------------|
| `subscribe(subscriber)` | Add a subscriber for updates |
| `update(data)` | Notify subscribers and write context |
| `get_name()` | Get data source name |
| `set_progress_callback(callback)` | Set progress update handler |
| `set_status_callback(callback)` | Set status update handler |

## Real-World Implementation Example

### ChatGPT API Data Source

```python
from subjective_abstract_data_source_package import SubjectiveOnDemandDataSource
import requests
from typing import Any


class SubjectiveChatGPTDataSource(SubjectiveOnDemandDataSource):
    """
    OnDemand data source for ChatGPT API interactions.
    """

    def __init__(self, name=None, session=None, dependency_data_sources=None,
                 subscribers=None, params=None):
        super().__init__(
            name=name,
            session=session,
            dependency_data_sources=dependency_data_sources,
            subscribers=subscribers,
            params=params
        )
        self.api_key = self.params.get("api_key")
        self.model = self.params.get("model", "gpt-3.5-turbo")
        self.api_url = "https://api.openai.com/v1/chat/completions"

    def _process_message(self, message: Any) -> Any:
        """Send message to ChatGPT and return response."""

        # Build messages from conversation history
        messages = []
        for entry in self._conversation_history[-10:]:  # Last 10 messages
            if entry["role"] in ("user", "assistant"):
                messages.append({
                    "role": entry["role"],
                    "content": str(entry["content"])
                })

        # Add current message
        messages.append({"role": "user", "content": str(message)})

        # Call API
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": self.model,
            "messages": messages
        }

        response = requests.post(self.api_url, headers=headers, json=payload)
        response.raise_for_status()

        data = response.json()
        return data["choices"][0]["message"]["content"]

    def get_icon(self) -> str:
        return '''<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <circle cx="12" cy="12" r="10" fill="#10A37F"/>
            <path d="M12 6v6l4 2" stroke="white" stroke-width="2"
                  stroke-linecap="round"/>
        </svg>'''

    def get_connection_data(self) -> dict:
        return {
            "connection_type": "ON_DEMAND",
            "fields": [
                {
                    "name": "api_key",
                    "type": "password",
                    "label": "OpenAI API Key",
                    "required": True
                },
                {
                    "name": "model",
                    "type": "select",
                    "label": "Model",
                    "required": False,
                    "options": ["gpt-3.5-turbo", "gpt-4", "gpt-4-turbo"]
                }
            ]
        }
```

## Error Handling

The base class handles errors in `_process_message` automatically:

- Errors are logged via BBLogger
- Error responses are added to conversation history with role "error"
- Response callback receives error object: `{"error": "message", "message": original_message}`

For custom error handling, wrap your logic in try/except:

```python
def _process_message(self, message: Any) -> Any:
    try:
        # Your processing logic
        result = self._call_api(message)
        return result
    except requests.RequestException as e:
        # Return structured error response
        return {
            "error": True,
            "error_type": "api_error",
            "message": str(e),
            "original_message": message
        }
```

## Checklist for Implementation

- [ ] Class inherits from `SubjectiveOnDemandDataSource`
- [ ] `_process_message()` implemented with message handling logic
- [ ] `get_icon()` returns valid SVG string
- [ ] `get_connection_data()` returns proper metadata dict
- [ ] Constructor calls `super().__init__()` with all parameters
- [ ] Custom params are extracted in constructor
- [ ] Error cases are handled appropriately
- [ ] Class has descriptive docstring
