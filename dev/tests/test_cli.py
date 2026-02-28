"""Test CLI commands via Typer's CliRunner.

All tests use --catalog local which resolves via the temp config file
created in conftest.py (config_file fixture is session-scoped + autouse).
"""

from typer.testing import CliRunner

from iceberg_meta.cli import app

runner = CliRunner()

CATALOG_ARGS = ["--catalog", "local"]


# ── sales.orders (partitioned, properties, append + overwrite) ────────────


def test_table_info(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "table-info", "sales.orders"])
    assert result.exit_code == 0, result.stdout
    assert "Table Info" in result.stdout


def test_table_info_shows_partition_spec(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "table-info", "sales.orders"])
    assert result.exit_code == 0, result.stdout
    assert "Partition Spec" in result.stdout
    assert "region" in result.stdout


def test_table_info_shows_properties(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "table-info", "sales.orders"])
    assert result.exit_code == 0, result.stdout
    assert "owner" in result.stdout
    assert "analytics-team" in result.stdout


def test_snapshots(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "snapshots", "sales.orders"])
    assert result.exit_code == 0, result.stdout
    assert "Snapshot" in result.stdout


def test_snapshots_shows_operation(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "snapshots", "sales.orders"])
    assert result.exit_code == 0, result.stdout
    assert "append" in result.stdout or "overwrite" in result.stdout


def test_schema(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "schema", "sales.orders"])
    assert result.exit_code == 0, result.stdout
    assert "order_id" in result.stdout
    assert "region" in result.stdout
    assert "amount" in result.stdout


def test_schema_history(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "schema", "sales.orders", "--history"])
    assert result.exit_code == 0, result.stdout
    assert "current" in result.stdout


def test_manifests(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "manifests", "sales.orders"])
    assert result.exit_code == 0, result.stdout
    assert "Manifest" in result.stdout


def test_files(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "files", "sales.orders"])
    assert result.exit_code == 0, result.stdout
    assert "Total files" in result.stdout


def test_partitions(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "partitions", "sales.orders"])
    assert result.exit_code == 0, result.stdout


def test_tree(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "tree", "sales.orders"])
    assert result.exit_code == 0, result.stdout
    assert "Table:" in result.stdout
    assert "Snapshot" in result.stdout


# ── sales.customers (schema evolution) ────────────────────────────────────


def test_schema_history_evolved(catalog):
    """Evolved table should expose multiple schema versions."""
    result = runner.invoke(
        app, [*CATALOG_ARGS, "schema", "sales.customers", "--history"],
    )
    assert result.exit_code == 0, result.stdout
    assert "Schema" in result.stdout
    assert "customer_id" in result.stdout


def test_customers_snapshots(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "snapshots", "sales.customers"])
    assert result.exit_code == 0, result.stdout
    assert "append" in result.stdout


def test_customers_tree(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "tree", "sales.customers"])
    assert result.exit_code == 0, result.stdout
    assert "Snapshot" in result.stdout


# ── analytics.events (nested struct, bucket partition) ────────────────────


def test_schema_nested_struct(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "schema", "analytics.events"])
    assert result.exit_code == 0, result.stdout
    assert "device" in result.stdout
    assert "browser" in result.stdout
    assert "os" in result.stdout


def test_events_snapshots(catalog):
    """Events table has 5 snapshots — all appends."""
    result = runner.invoke(app, [*CATALOG_ARGS, "snapshots", "analytics.events"])
    assert result.exit_code == 0, result.stdout
    assert "append" in result.stdout


def test_events_tree(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "tree", "analytics.events"])
    assert result.exit_code == 0, result.stdout
    assert "Manifest" in result.stdout


def test_events_files(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "files", "analytics.events"])
    assert result.exit_code == 0, result.stdout
    assert "Total files" in result.stdout


# ── analytics.sessions (delete operation) ─────────────────────────────────


def test_sessions_snapshots(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "snapshots", "analytics.sessions"])
    assert result.exit_code == 0, result.stdout
    assert "Snapshot" in result.stdout


def test_manifests_specific_snapshot(catalog, sessions_table):
    """Request manifests for a specific snapshot by ID."""
    snap_id = sessions_table.metadata.snapshots[0].snapshot_id
    result = runner.invoke(app, [
        *CATALOG_ARGS, "manifests", "analytics.sessions",
        "--snapshot-id", str(snap_id),
    ])
    assert result.exit_code == 0, result.stdout
    assert "Manifest" in result.stdout


def test_files_with_snapshot_filter(catalog, orders_table):
    """Request files for a specific snapshot."""
    snap_id = orders_table.metadata.snapshots[0].snapshot_id
    result = runner.invoke(app, [
        *CATALOG_ARGS, "files", "sales.orders",
        "--snapshot-id", str(snap_id),
    ])
    assert result.exit_code == 0, result.stdout
    assert "Total files" in result.stdout


# ── staging.landing_raw (empty table) ─────────────────────────────────────


def test_empty_table_snapshots(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "snapshots", "staging.landing_raw"])
    assert result.exit_code == 0, result.stdout


def test_empty_table_tree(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "tree", "staging.landing_raw"])
    assert result.exit_code == 0, result.stdout
    assert "no snapshots" in result.stdout.lower()


def test_empty_table_manifests(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "manifests", "staging.landing_raw"])
    assert result.exit_code == 0, result.stdout
    assert "no snapshots" in result.stdout.lower()


def test_empty_table_schema(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "schema", "staging.landing_raw"])
    assert result.exit_code == 0, result.stdout
    assert "raw_id" in result.stdout
    assert "payload" in result.stdout


# ── staging.wide_metrics (22-column schema) ───────────────────────────────


def test_wide_schema(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "schema", "staging.wide_metrics"])
    assert result.exit_code == 0, result.stdout
    assert "cpu_pct" in result.stdout
    assert "heap_used_mb" in result.stdout
    assert "collected_at" in result.stdout


def test_wide_files(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "files", "staging.wide_metrics"])
    assert result.exit_code == 0, result.stdout
    assert "Total files" in result.stdout


def test_wide_table_info(catalog):
    result = runner.invoke(app, [*CATALOG_ARGS, "table-info", "staging.wide_metrics"])
    assert result.exit_code == 0, result.stdout
    assert "Table Info" in result.stdout


# ── general ───────────────────────────────────────────────────────────────


def test_no_args_shows_help():
    result = runner.invoke(app, [])
    assert result.exit_code in (0, 2)
    assert "Explore Apache Iceberg" in result.stdout or "Usage" in result.stdout
