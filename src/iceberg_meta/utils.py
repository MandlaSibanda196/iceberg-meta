"""Utility functions for formatting and conversion."""

from __future__ import annotations

from datetime import datetime, timezone


def format_bytes(num_bytes: int | float) -> str:
    """Convert bytes to human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"


def format_timestamp_ms(timestamp_ms: int | None) -> str:
    """Convert epoch milliseconds to readable datetime string."""
    if timestamp_ms is None:
        return "unknown"
    try:
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except (OSError, ValueError, OverflowError):
        return "unknown"


def truncate_path(path: str | None, max_length: int = 60) -> str:
    """Truncate a file path for display, keeping the filename visible."""
    if not path:
        return ""
    if len(path) <= max_length:
        return path
    filename = path.rsplit("/", 1)[-1]
    return f".../{filename}"
