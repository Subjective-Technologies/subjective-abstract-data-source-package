"""
Shared pipeline-level ticker config normalization (JSON / runner parity).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

PIPELINE_TICKER_INTERVAL_UNITS = ("ms", "sec", "min", "hour")
PIPELINE_TICKER_DEFAULTS: Dict[str, Any] = {
    "enabled": False,
    "mode": "interval",
    "interval_value": 60,
    "interval_unit": "sec",
    "cron_expression": "",
    "immediate_first_tick": False,
}


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    if value is None:
        return default
    return bool(value)


def default_pipeline_ticker_config() -> Dict[str, Any]:
    return dict(PIPELINE_TICKER_DEFAULTS)


def normalize_pipeline_ticker_config(ticker_config: Any) -> Dict[str, Any]:
    normalized = default_pipeline_ticker_config()
    if not isinstance(ticker_config, dict):
        return normalized

    normalized["enabled"] = _coerce_bool(
        ticker_config.get("enabled"),
        default=bool(PIPELINE_TICKER_DEFAULTS["enabled"]),
    )
    normalized["mode"] = str(
        ticker_config.get("mode") or PIPELINE_TICKER_DEFAULTS["mode"]
    ).strip().lower() or str(PIPELINE_TICKER_DEFAULTS["mode"])

    interval_value = ticker_config.get("interval_value", PIPELINE_TICKER_DEFAULTS["interval_value"])
    try:
        normalized["interval_value"] = int(float(interval_value))
    except (TypeError, ValueError):
        normalized["interval_value"] = int(PIPELINE_TICKER_DEFAULTS["interval_value"])

    normalized["interval_unit"] = str(
        ticker_config.get("interval_unit") or PIPELINE_TICKER_DEFAULTS["interval_unit"]
    ).strip().lower() or str(PIPELINE_TICKER_DEFAULTS["interval_unit"])
    normalized["cron_expression"] = str(ticker_config.get("cron_expression") or "").strip()
    normalized["immediate_first_tick"] = _coerce_bool(
        ticker_config.get("immediate_first_tick"),
        default=bool(PIPELINE_TICKER_DEFAULTS["immediate_first_tick"]),
    )
    return normalized


def should_serialize_pipeline_ticker(ticker_config: Any) -> bool:
    normalized = normalize_pipeline_ticker_config(ticker_config)
    return any(
        normalized.get(key) != PIPELINE_TICKER_DEFAULTS.get(key)
        for key in PIPELINE_TICKER_DEFAULTS
    )


def describe_pipeline_ticker(ticker_config: Any) -> str:
    normalized = normalize_pipeline_ticker_config(ticker_config)
    if not normalized.get("enabled"):
        return "Ticker: off"
    if normalized.get("mode") == "cron":
        expr = str(normalized.get("cron_expression") or "").strip() or "<missing cron>"
        return f"Ticker: cron {expr}"
    value = int(normalized.get("interval_value") or 0)
    unit = str(normalized.get("interval_unit") or "sec")
    suffix = " (immediate first tick)" if normalized.get("immediate_first_tick") else ""
    return f"Ticker: every {value} {unit}{suffix}"


def validate_pipeline_ticker_config(ticker_config: Any) -> List[str]:
    if ticker_config is None:
        return []
    if not isinstance(ticker_config, dict):
        return ["Top-level 'ticker' must be an object."]

    errors: List[str] = []
    normalized = normalize_pipeline_ticker_config(ticker_config)
    mode = str(normalized.get("mode") or "").strip().lower()
    if mode not in {"interval", "cron"}:
        errors.append("Ticker mode must be either 'interval' or 'cron'.")

    if mode == "interval":
        if int(normalized.get("interval_value") or 0) <= 0:
            errors.append("Ticker interval_value must be greater than 0.")
        if str(normalized.get("interval_unit") or "").strip().lower() not in PIPELINE_TICKER_INTERVAL_UNITS:
            errors.append(
                "Ticker interval_unit must be one of: "
                + ", ".join(PIPELINE_TICKER_INTERVAL_UNITS)
                + "."
            )
    elif mode == "cron":
        cron_expression = str(normalized.get("cron_expression") or "").strip()
        if not cron_expression:
            errors.append("Ticker cron_expression is required when mode is 'cron'.")
        else:
            try:
                from croniter import croniter
            except ImportError:
                errors.append("Ticker cron mode requires the 'croniter' package to be installed.")
            else:
                try:
                    croniter(cron_expression, datetime.now())
                except Exception as exc:
                    errors.append(f"Ticker cron_expression is invalid: {exc}")

    return errors
