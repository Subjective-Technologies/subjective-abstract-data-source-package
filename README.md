# Subjective Abstract Data Source Package

A Python package providing abstract base classes for building data source addons for BrainBoost data pipelines software.

## Overview

This package contains two main abstract classes that developers can subclass to create custom data sources for the BrainBoost data pipeline system:

- **`SubjectiveDataSource`**: Base class for standard data sources
- **`SubjectiveRealTimeDataSource`**: Base class for real-time data sources with async capabilities

## Installation

### From PyPI (when published)
```bash
pip install subjective-abstract-data-source-package
```

### From source
```bash
git clone https://github.com/brainboost/subjective-abstract-data-source-package.git
cd subjective-abstract-data-source-package
pip install -e .
```

### Development installation
```bash
pip install -e ".[dev]"
```

## Quick Start

### Creating a Standard Data Source

```python
from subjective_abstract_data_source_package import SubjectiveDataSource

class MyCustomDataSource(SubjectiveDataSource):
    def fetch(self):
        """Implement your data fetching logic here."""
        # Your custom implementation
        data = self._get_data_from_source()
        self.update(data)  # Notify subscribers
    
    def get_icon(self):
        """Return SVG code for your data source icon."""
        return """
        <svg width="24" height="24" viewBox="0 0 24 24">
            <circle cx="12" cy="12" r="10" fill="blue"/>
        </svg>
        """
    
    def get_connection_data(self):
        """Return connection configuration for your data source."""
        return {
            "connection_type": "CUSTOM",
            "fields": ["api_key", "endpoint", "timeout"]
        }
```

### Creating a Real-Time Data Source

```python
from subjective_abstract_data_source_package import SubjectiveRealTimeDataSource

class MyRealTimeDataSource(SubjectiveRealTimeDataSource):
    def fetch(self):
        """Implement your real-time data fetching logic here."""
        # Real-time data sources typically don't need fetch implementation
        # as they handle data streaming through the async server
        pass
    
    def get_icon(self):
        """Return SVG code for your real-time data source icon."""
        return """
        <svg width="24" height="24" viewBox="0 0 24 24">
            <rect x="2" y="2" width="20" height="20" fill="green"/>
        </svg>
        """
    
    def get_connection_data(self):
        """Return connection configuration for your real-time data source."""
        return {
            "connection_type": "REALTIME",
            "fields": ["websocket_url", "api_key"]
        }
```

## Features

### SubjectiveDataSource
- **Progress Tracking**: Built-in progress monitoring with callbacks
- **Dependency Management**: Support for dependent data sources
- **Subscriber Pattern**: Observer pattern for data updates
- **Status Callbacks**: Real-time status updates
- **Connection Configuration**: Standardized connection data format

### SubjectiveRealTimeDataSource
- **Async Server**: Built-in asyncio server for real-time data streaming
- **Client Management**: Automatic client connection handling
- **JSON Data Processing**: Automatic JSON parsing and validation
- **Thread Safety**: Thread-safe operations with separate event loop
- **Graceful Shutdown**: Proper cleanup of connections and resources

## API Reference

### SubjectiveDataSource

#### Core Methods
- `fetch()`: Abstract method to implement data fetching logic
- `get_icon()`: Abstract method to return SVG icon code
- `get_connection_data()`: Abstract method to return connection configuration

#### Progress Tracking
- `set_progress_callback(callback)`: Set progress update callback
- `set_status_callback(callback)`: Set status update callback
- `estimated_remaining_time()`: Get estimated time to completion
- `get_total_to_process()`: Get total items to process
- `get_total_processed()`: Get number of processed items

#### Subscription Management
- `subscribe(subscriber)`: Add a subscriber for data updates
- `update(data)`: Notify all subscribers with new data

### SubjectiveRealTimeDataSource

#### Real-Time Features
- `subscribe(subscriber)`: Subscribe and start the real-time server
- `stop()`: Gracefully stop the real-time server
- `_start_server()`: Start the async server (called automatically)

## Development

### Running Tests
```bash
pytest
```

### Code Formatting
```bash
black .
```

### Type Checking
```bash
mypy .
```

### Linting
```bash
flake8 .
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For support and questions:
- Email: pablo.borda@subjectivetechnologies.com
- Issues: [GitHub Issues](https://github.com/brainboost/subjective-abstract-data-source-package/issues)
- Documentation: [Read the Docs](https://subjective-abstract-data-source-package.readthedocs.io/)

## Changelog

### 1.0.0
- Initial release
- Abstract base classes for data sources
- Real-time data source support with async capabilities
- Progress tracking and status callbacks
- Comprehensive documentation and examples 