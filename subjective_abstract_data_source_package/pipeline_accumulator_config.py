"""
Shared workflow accumulator config normalization (UI / JSON / runner parity).
"""

from __future__ import annotations

from typing import Any, Dict, List

PIPELINE_ACCUMULATOR_RELEASE_MODES = ("array", "concatenated")
PIPELINE_ACCUMULATOR_DEFAULTS: Dict[str, Any] = {
    "threshold": 5,
    "release_mode": "array",
    "separator": "\n",
}


def default_pipeline_accumulator_config() -> Dict[str, Any]:
    return dict(PIPELINE_ACCUMULATOR_DEFAULTS)


def normalize_pipeline_accumulator_config(accumulator_config: Any) -> Dict[str, Any]:
    normalized = default_pipeline_accumulator_config()
    if not isinstance(accumulator_config, dict):
        return normalized

    threshold = accumulator_config.get(
        "threshold",
        PIPELINE_ACCUMULATOR_DEFAULTS["threshold"],
    )
    try:
        normalized["threshold"] = int(float(threshold))
    except (TypeError, ValueError):
        normalized["threshold"] = int(PIPELINE_ACCUMULATOR_DEFAULTS["threshold"])

    normalized["release_mode"] = str(
        accumulator_config.get("release_mode")
        or PIPELINE_ACCUMULATOR_DEFAULTS["release_mode"]
    ).strip().lower() or str(PIPELINE_ACCUMULATOR_DEFAULTS["release_mode"])
    normalized["separator"] = str(
        accumulator_config.get("separator")
        if accumulator_config.get("separator") is not None
        else PIPELINE_ACCUMULATOR_DEFAULTS["separator"]
    )
    return normalized


def describe_pipeline_accumulator(accumulator_config: Any) -> str:
    normalized = normalize_pipeline_accumulator_config(accumulator_config)
    return (
        f"Threshold: {int(normalized.get('threshold') or 0)}"
        f" | Mode: {str(normalized.get('release_mode') or 'array')}"
    )


def validate_pipeline_accumulator_config(accumulator_config: Any) -> List[str]:
    if accumulator_config is None:
        return []
    if not isinstance(accumulator_config, dict):
        return ["Accumulator internal_data must be an object."]

    errors: List[str] = []
    normalized = normalize_pipeline_accumulator_config(accumulator_config)
    if int(normalized.get("threshold") or 0) <= 0:
        errors.append("Accumulator threshold must be greater than 0.")

    release_mode = str(normalized.get("release_mode") or "").strip().lower()
    if release_mode not in PIPELINE_ACCUMULATOR_RELEASE_MODES:
        errors.append(
            "Accumulator release_mode must be one of: "
            + ", ".join(PIPELINE_ACCUMULATOR_RELEASE_MODES)
            + "."
        )

    separator = normalized.get("separator")
    if release_mode == "concatenated" and not isinstance(separator, str):
        errors.append("Accumulator separator must be a string.")

    return errors
