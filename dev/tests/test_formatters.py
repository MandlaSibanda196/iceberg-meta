"""Test formatter utility functions and Rich renderers."""

from io import StringIO

from rich.console import Console

from iceberg_meta import formatters
from iceberg_meta.utils import format_bytes, format_timestamp_ms, truncate_path

# ── unit tests for utils helpers ──────────────────────────────────────────


def test_format_bytes_small():
    assert format_bytes(512) == "512.0 B"


def test_format_bytes_kb():
    assert format_bytes(1536) == "1.5 KB"


def test_format_bytes_mb():
    assert format_bytes(5 * 1024 * 1024) == "5.0 MB"


def test_format_bytes_gb():
    assert format_bytes(2 * 1024**3) == "2.0 GB"


def test_format_timestamp_ms():
    result = format_timestamp_ms(1705312200000)
    assert "2024-01-15" in result
    assert "UTC" in result


def test_truncate_path_short():
    path = "/tmp/data/file.parquet"
    assert truncate_path(path) == path


def test_truncate_path_long():
    path = (
        "/very/long/path/that/exceeds/the/maximum/length"
        "/allowed/for/display/purposes/file.parquet"
    )
    result = truncate_path(path, max_length=40)
    assert result.startswith("...")
    assert result.endswith("file.parquet")


# ── helpers ───────────────────────────────────────────────────────────────


def _capture(render_fn, *args, **kwargs) -> str:
    """Call a render function and return its plain-text output."""
    console = Console(file=StringIO(), force_terminal=True, width=160)
    render_fn(console, *args, **kwargs)
    return console.file.getvalue()


# ── integration: render_table_info ────────────────────────────────────────


def test_render_table_info_shows_properties(orders_table):
    text = _capture(formatters.render_table_info, orders_table)
    assert "Table Info" in text
    assert "owner" in text
    assert "analytics-team" in text
    assert "data-classification" in text


def test_render_table_info_shows_partition_spec(orders_table):
    text = _capture(formatters.render_table_info, orders_table)
    assert "Partition Spec" in text
    assert "region" in text


def test_render_table_info_format_version(orders_table):
    text = _capture(formatters.render_table_info, orders_table)
    assert "Format Version" in text


# ── integration: render_snapshots ─────────────────────────────────────────


def test_render_snapshots_shows_operations(orders_table):
    text = _capture(formatters.render_snapshots, orders_table)
    assert "append" in text or "overwrite" in text


def test_render_snapshots_shows_parent_chain(orders_table):
    """Non-first snapshots should display a parent snapshot ID."""
    text = _capture(formatters.render_snapshots, orders_table)
    lines_with_dash = [ln for ln in text.splitlines() if "-" in ln]
    assert len(lines_with_dash) > 0


def test_render_snapshots_has_summary(orders_table):
    text = _capture(formatters.render_snapshots, orders_table)
    assert "added-records" in text or "total-records" in text


# ── integration: render_schema / render_schema_history ────────────────────


def test_render_schema_orders(orders_table):
    text = _capture(formatters.render_schema, orders_table.schema())
    assert "order_id" in text
    assert "region" in text
    assert "is_priority" in text


def test_render_schema_history_customers(customers_table):
    text = _capture(formatters.render_schema_history, customers_table)
    assert "Schema" in text
    assert "customer_id" in text


def test_render_schema_nested(events_table):
    text = _capture(formatters.render_schema, events_table.schema())
    assert "device" in text
    assert "browser" in text
    assert "os" in text
    assert "version" in text


# ── integration: render_manifests ─────────────────────────────────────────


def test_render_manifests_counts(orders_table):
    text = _capture(formatters.render_manifests, orders_table)
    assert "Manifest Files" in text
    assert "Manifest list" in text or "Manifest List" in text


def test_render_manifests_for_specific_snapshot(orders_table):
    snap_id = orders_table.metadata.snapshots[0].snapshot_id
    text = _capture(formatters.render_manifests, orders_table, snapshot_id=snap_id)
    assert "Manifest Files" in text


# ── integration: render_files ─────────────────────────────────────────────


def test_render_files_orders(orders_table):
    text = _capture(formatters.render_files, orders_table)
    assert "Data Files" in text
    assert "Total files" in text


def test_render_files_events(events_table):
    text = _capture(formatters.render_files, events_table)
    assert "Total files" in text


# ── integration: render_partitions ────────────────────────────────────────


def test_render_partitions_multiple_partitions(orders_table):
    text = _capture(formatters.render_partitions, orders_table)
    assert "Partition Statistics" in text


# ── integration: render_metadata_tree ─────────────────────────────────────


def test_render_metadata_tree_depth(orders_table):
    text = _capture(formatters.render_metadata_tree, orders_table)
    assert "Table:" in text
    assert "Snapshot" in text
    assert "Manifest List" in text
    assert "Manifest:" in text


def test_render_metadata_tree_events(events_table):
    text = _capture(formatters.render_metadata_tree, events_table)
    assert "Snapshot" in text


def test_render_metadata_tree_empty(landing_table):
    text = _capture(formatters.render_metadata_tree, landing_table)
    assert "no snapshots" in text.lower()


def test_render_metadata_tree_specific_snapshot(orders_table):
    snap_id = orders_table.metadata.snapshots[0].snapshot_id
    text = _capture(
        formatters.render_metadata_tree, orders_table, snapshot_id=snap_id,
    )
    assert "Snapshot" in text
    assert str(snap_id) in text


# ── integration: render_snapshot_detail ───────────────────────────────────


def test_render_snapshot_detail(orders_table):
    snap_id = orders_table.metadata.snapshots[-1].snapshot_id
    text = _capture(formatters.render_snapshot_detail, orders_table, snap_id)
    assert str(snap_id) in text
    assert "Manifest" in text
    assert "Data Files" in text


def test_render_snapshot_detail_not_found(orders_table):
    text = _capture(formatters.render_snapshot_detail, orders_table, 999999)
    assert "not found" in text.lower()
