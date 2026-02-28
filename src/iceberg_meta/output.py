"""Output formatting for JSON, CSV, and Rich table modes."""

from __future__ import annotations

import csv
import io
import json
from enum import Enum
from typing import Any

from rich.console import Console
from rich.table import Table as RichTable


class OutputFormat(str, Enum):
    TABLE = "table"
    JSON = "json"
    CSV = "csv"


def emit(
    console: Console,
    data: list[dict[str, Any]],
    columns: list[str],
    fmt: OutputFormat,
    *,
    title: str = "",
    column_styles: dict[str, dict[str, Any]] | None = None,
) -> None:
    """Emit data in the requested format.

    Args:
        console: Rich console for table output.
        data: List of row dicts.
        columns: Ordered column names to display.
        fmt: Output format (table, json, csv).
        title: Title for Rich table mode.
        column_styles: Per-column keyword args for RichTable.add_column
            (e.g. ``{"Size": {"justify": "right", "style": "green"}}``).
    """
    if fmt == OutputFormat.JSON:
        _emit_json(data, columns)
    elif fmt == OutputFormat.CSV:
        _emit_csv(data, columns)
    else:
        _emit_table(console, data, columns, title=title, column_styles=column_styles)


def _emit_json(data: list[dict[str, Any]], columns: list[str]) -> None:
    filtered = [{col: row.get(col) for col in columns} for row in data]
    print(json.dumps(filtered, indent=2, default=str))


def _emit_csv(data: list[dict[str, Any]], columns: list[str]) -> None:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    for row in data:
        writer.writerow({col: row.get(col, "") for col in columns})
    print(buf.getvalue(), end="")


def _emit_table(
    console: Console,
    data: list[dict[str, Any]],
    columns: list[str],
    *,
    title: str = "",
    column_styles: dict[str, dict[str, Any]] | None = None,
) -> None:
    column_styles = column_styles or {}
    table = RichTable(title=title, show_lines=True)
    for col in columns:
        kwargs = column_styles.get(col, {})
        table.add_column(col, **kwargs)
    for row in data:
        table.add_row(*(str(row.get(col, "")) for col in columns))
    console.print(table)
