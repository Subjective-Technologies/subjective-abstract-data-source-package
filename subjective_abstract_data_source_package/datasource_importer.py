"""
DataSource Importer Utility

Provides a reusable function to import data source classes from various locations:
- data_source_addons (legacy)
- data_source_installed_plugins (current plugin system)
- subjective_abstract_data_source_package (base classes)

This logic is used by both datasource_launcher.py and SubjectivePipelineDataSource.
"""

import os
import sys
from importlib import import_module
from typing import Type, Optional

# Try to import BBLogger and BBConfig, but provide fallbacks
try:
    from brainboost_data_source_logger_package.BBLogger import BBLogger
except ImportError:
    class BBLogger:
        @staticmethod
        def log(msg):
            print(f"[DataSourceImporter] {msg}")

try:
    from brainboost_configuration_package.BBConfig import BBConfig
except ImportError:
    try:
        from brainboost_config_package.BBConfig import BBConfig
    except ImportError:
        class BBConfig:
            @staticmethod
            def get(key, default):
                return default


_DATASOURCE_CLASS_CACHE: dict[tuple[str, str], Type] = {}
_PLUGIN_LOCATION_CACHE: dict[tuple[str, str], Optional[tuple[str, str, str]]] = {}


def _find_project_root(start_dir: str) -> str:
    """Walk upwards to find a Subjective project root."""
    current = os.path.abspath(start_dir)
    fallback_userdata_root = None
    while True:
        if os.path.isfile(os.path.join(current, "subjective.conf")):
            return current
        if fallback_userdata_root is None and os.path.isdir(os.path.join(current, "com_subjective_userdata")):
            fallback_userdata_root = current
        parent = os.path.dirname(current)
        if parent == current:
            return fallback_userdata_root or os.path.abspath(start_dir)
        current = parent


def calculate_abs_path(path: str, base_dir: Optional[str] = None) -> str:
    """Calculate absolute path, handling relative paths."""
    path = os.path.expandvars(path)
    if os.path.isabs(path):
        return path
    base = os.path.abspath(base_dir or os.getcwd())
    return os.path.abspath(os.path.join(base, path))


def _resolve_installed_plugins_dir(project_root: str) -> str:
    try:
        configured_plugins_dir = BBConfig.get("DATASOURCES_PLUGIN_PATH", "")
    except Exception as cfg_err:
        BBLogger.log(
            "BBConfig unavailable while resolving DATASOURCES_PLUGIN_PATH "
            f"({cfg_err}); falling back to environment/default plugin path."
        )
        configured_plugins_dir = os.environ.get("DATASOURCES_PLUGIN_PATH", "")
    if configured_plugins_dir:
        configured_plugins_dir = os.path.expandvars(str(configured_plugins_dir))
        if "${USERDATA_PATH}" in configured_plugins_dir:
            configured_plugins_dir = configured_plugins_dir.replace(
                "${USERDATA_PATH}",
                os.path.join(project_root, "com_subjective_userdata")
            )
        installed_plugins_dir = calculate_abs_path(configured_plugins_dir, base_dir=project_root)
    else:
        installed_plugins_dir = os.path.join(project_root, "com_subjective_userdata", "com_subjective_plugins")
        if not os.path.isdir(installed_plugins_dir):
            installed_plugins_dir = os.path.join(project_root, "data_source_installed_plugins")
    return calculate_abs_path(str(installed_plugins_dir), base_dir=project_root)


def _find_plugin_module_for_class(
    ds_class_name: str,
    installed_plugins_dir: str,
) -> Optional[tuple[str, str, str]]:
    cache_key = (os.path.abspath(installed_plugins_dir), ds_class_name)
    if cache_key in _PLUGIN_LOCATION_CACHE:
        cached = _PLUGIN_LOCATION_CACHE[cache_key]
        BBLogger.log(f"Using cached plugin location for {ds_class_name}: {cached}")
        return cached

    BBLogger.log(f"Scanning installed plugins directory: {installed_plugins_dir}")
    result = None

    if os.path.exists(installed_plugins_dir):
        for plugin_dir in os.listdir(installed_plugins_dir):
            plugin_path = os.path.join(installed_plugins_dir, plugin_dir)
            if not os.path.isdir(plugin_path):
                continue
            for file in os.listdir(plugin_path):
                if not file.endswith("DataSource.py"):
                    continue
                file_path = os.path.join(plugin_path, file)
                try:
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                        file_content = f.read()
                except Exception as read_err:
                    BBLogger.log(f"Error reading {file_path}: {read_err}")
                    continue
                if f"class {ds_class_name}" in file_content:
                    BBLogger.log(f"Found {ds_class_name} in {file} in {plugin_dir}")
                    result = (plugin_path, file[:-3], file)
                    _PLUGIN_LOCATION_CACHE[cache_key] = result
                    return result
                pass  # Not in this file, continue scanning

    _PLUGIN_LOCATION_CACHE[cache_key] = None
    return None


def import_datasource_class(ds_class_name: str, project_root: Optional[str] = None) -> Type:
    """
    Import a data source class by name, searching multiple locations.

    Search order:
    1. data_source_addons (legacy location)
    2. data_source_installed_plugins (plugin directories)
    3. subjective_abstract_data_source_package (base classes)

    Args:
        ds_class_name: Name of the data source class to import
        project_root: Project root directory (default: current working directory)

    Returns:
        The imported data source class

    Raises:
        ImportError: If the class cannot be found in any location
    """
    if project_root is None:
        project_root = os.getcwd()
    project_root = _find_project_root(project_root)
    class_cache_key = (project_root, ds_class_name)

    if class_cache_key in _DATASOURCE_CLASS_CACHE:
        BBLogger.log(f"Using cached datasource class for {ds_class_name}")
        return _DATASOURCE_CLASS_CACHE[class_cache_key]

    ds_class = None

    # First try data_source_addons (legacy location)
    try:
        BBLogger.log(f"Attempting to import from data_source_addons.{ds_class_name}")
        module = import_module("data_source_addons." + ds_class_name)
        ds_class = getattr(module, ds_class_name)
        BBLogger.log(f"Successfully imported {ds_class_name} from data_source_addons")
        _DATASOURCE_CLASS_CACHE[class_cache_key] = ds_class
        return ds_class
    except ImportError as e:
        BBLogger.log(f"Legacy data_source_addons import skipped: {e}")

    # Try data_source_installed_plugins (new location)
    try:
        BBLogger.log(f"Attempting to import from data_source_installed_plugins")

        installed_plugins_dir = _resolve_installed_plugins_dir(project_root)
        plugin_location = _find_plugin_module_for_class(ds_class_name, installed_plugins_dir)
        if plugin_location is not None:
            plugin_path, module_name, file_name = plugin_location
            if plugin_path not in sys.path:
                sys.path.insert(0, plugin_path)
                BBLogger.log(f"Added {plugin_path} to sys.path")
            BBLogger.log(f"Attempting to import module: {module_name}")
            module = import_module(module_name)
            if hasattr(module, ds_class_name):
                ds_class = getattr(module, ds_class_name)
                BBLogger.log(f"Successfully imported {ds_class_name} from {os.path.basename(plugin_path)}/{file_name}")
                _DATASOURCE_CLASS_CACHE[class_cache_key] = ds_class
                return ds_class

        if not ds_class:
            raise ImportError(f"Could not find {ds_class_name} in any installed plugin")

    except Exception as e2:
        BBLogger.log(f"Failed to import from data_source_installed_plugins: {e2}")

        # Try importing from subjective_abstract_data_source_package (for pipeline and base classes)
        try:
            BBLogger.log(f"Attempting to import from subjective_abstract_data_source_package")
            module = import_module(f"subjective_abstract_data_source_package.{ds_class_name}")
            ds_class = getattr(module, ds_class_name)
            BBLogger.log(f"Successfully imported {ds_class_name} from subjective_abstract_data_source_package")
            _DATASOURCE_CLASS_CACHE[class_cache_key] = ds_class
            return ds_class
        except Exception as e3:
            BBLogger.log(f"Failed to import from subjective_abstract_data_source_package: {e3}")
            raise ImportError(
                f"Could not import {ds_class_name} from data_source_addons, "
                f"data_source_installed_plugins, or subjective_abstract_data_source_package. "
                f"Errors: {e2}, {e3}"
            )

    if not ds_class:
        raise ImportError(f"Could not find class {ds_class_name}")

    return ds_class
