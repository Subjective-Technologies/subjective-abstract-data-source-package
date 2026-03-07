import os
import re
import tempfile


TEMP_STORAGE_FOLDER_NAME = "com_subjective_data_sources_temp"

_CONNECTION_INVALID_CHARS_RE = re.compile(r"[^A-Za-z0-9 _-]+")
_SERVICE_INVALID_CHARS_RE = re.compile(r"[^A-Za-z0-9._-]+")
_REPEATED_UNDERSCORES_RE = re.compile(r"_+")
_WINDOWS_RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
}


def normalize_path(path_value: str, *, absolute: bool = True) -> str:
    text = "" if path_value is None else str(path_value).strip()
    if not text:
        return ""
    text = os.path.expandvars(os.path.expanduser(text))
    if absolute and not os.path.isabs(text):
        text = os.path.abspath(text)
    return os.path.normpath(text)


def resolve_default_tmp_base(userdata_path: str) -> str:
    userdata_root = normalize_path(userdata_path, absolute=True)
    if userdata_root:
        return os.path.join(userdata_root, TEMP_STORAGE_FOLDER_NAME)
    return os.path.join(tempfile.gettempdir(), TEMP_STORAGE_FOLDER_NAME)


def sanitize_connection_name(connection_name: str, *, max_length: int = 80) -> str:
    text = "" if connection_name is None else str(connection_name).strip()
    if not text:
        text = "connection"
    text = _CONNECTION_INVALID_CHARS_RE.sub("_", text)
    text = _REPEATED_UNDERSCORES_RE.sub("_", text)
    text = text.strip(" _.")
    if not text:
        text = "connection"
    if len(text) > max_length:
        text = text[:max_length].rstrip(" _.")
    if not text:
        text = "connection"
    if text.upper() in _WINDOWS_RESERVED_NAMES:
        text = f"{text}_"
    return text


def sanitize_service_key(service_key: str, *, default: str = "127.0.0.1") -> str:
    text = "" if service_key is None else str(service_key).strip()
    if not text:
        text = default
    text = _SERVICE_INVALID_CHARS_RE.sub("_", text)
    text = _REPEATED_UNDERSCORES_RE.sub("_", text)
    text = text.strip(" ._-")
    if not text:
        text = default
    return text


def derive_service_key(service_id: str | None = None, manager_ip: str | None = None) -> str:
    if service_id:
        return sanitize_service_key(service_id)
    if manager_ip:
        return sanitize_service_key(manager_ip)
    return sanitize_service_key("127.0.0.1")


def build_effective_tmp_root(base_path: str, service_key: str) -> str:
    normalized_base = normalize_path(base_path, absolute=True)
    if not normalized_base:
        normalized_base = resolve_default_tmp_base("")
    normalized_service_key = sanitize_service_key(service_key)
    base_name = os.path.basename(os.path.normpath(normalized_base)).lower()
    if base_name == TEMP_STORAGE_FOLDER_NAME.lower():
        return os.path.normpath(os.path.join(normalized_base, normalized_service_key))
    return os.path.normpath(
        os.path.join(normalized_base, normalized_service_key, TEMP_STORAGE_FOLDER_NAME)
    )


def build_connection_tmp_dir(tmp_root: str, connection_name: str) -> str:
    normalized_root = normalize_path(tmp_root, absolute=True)
    sanitized_connection_name = sanitize_connection_name(connection_name)
    return os.path.normpath(os.path.join(normalized_root, sanitized_connection_name))


def build_process_tmp_root(process_id: int | None = None) -> str:
    pid = int(process_id or os.getpid())
    return os.path.normpath(
        os.path.join(tempfile.gettempdir(), f"subjective_ds_tmp_{pid}", TEMP_STORAGE_FOLDER_NAME)
    )
