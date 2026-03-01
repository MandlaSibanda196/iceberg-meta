
import pytest
from typer.testing import CliRunner
from iceberg_meta.cli import app
from iceberg_meta.demo import create_demo_catalog, cleanup_demo

runner = CliRunner()

def test_cli_list_tables(demo_catalog):
    # The fixture gives us a catalog config object, but the CLI reads from env or file.
    # We can pass --uri to override.
    
    uri = demo_catalog.properties["uri"]
    warehouse = demo_catalog.properties["warehouse"]
    
    result = runner.invoke(app, ["--catalog", "demo", "--uri", uri, "--warehouse", warehouse, "list-tables"])
    if result.exit_code != 0:
        print(f"Stdout: {result.stdout}")
        print(f"Exception: {result.exception}")
    assert result.exit_code == 0
    assert "sales.orders" in result.stdout
    assert "analytics.events" in result.stdout

def test_cli_health(demo_catalog):
    uri = demo_catalog.properties["uri"]
    warehouse = demo_catalog.properties["warehouse"]
    
    result = runner.invoke(app, ["--catalog", "demo", "--uri", uri, "--warehouse", warehouse, "health", "sales.orders"])
    assert result.exit_code == 0
    assert "File Health" in result.stdout
    assert "Column Null Rates" in result.stdout

def test_cli_missing_table(demo_catalog):
    uri = demo_catalog.properties["uri"]
    warehouse = demo_catalog.properties["warehouse"]
    
    result = runner.invoke(app, ["--catalog", "demo", "--uri", uri, "--warehouse", warehouse, "health", "sales.missing_table"])
    assert result.exit_code == 1
    # assert "Table not found" in result.stdout  # TODO: Fix capturing stderr from rich Console

def test_cli_health_namespace(demo_catalog):
    """Verify health scan works on a namespace."""
    uri = demo_catalog.properties["uri"]
    warehouse = demo_catalog.properties["warehouse"]
    
    result = runner.invoke(app, ["--catalog", "demo", "--uri", uri, "--warehouse", warehouse, "health", "sales", "--namespace"])
    if result.exit_code != 0:
        print(f"Stdout: {result.stdout}")
        print(f"Exception: {result.exception}")
    assert result.exit_code == 0
    assert "sales.orders" in result.stdout
    assert "sales.customers" in result.stdout
    assert "File Health" in result.stdout
