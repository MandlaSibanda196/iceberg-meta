
import pytest
from iceberg_meta.catalog import get_table
from iceberg_meta.formatters import collect_table_health

def test_collect_table_health_orders(demo_catalog):
    """Verify health check runs on a populated table."""
    tbl = get_table(demo_catalog, "sales.orders")
    health = collect_table_health(tbl)
    
    assert "file_health" in health
    fh = health["file_health"]
    assert fh["file_count"] > 0
    assert fh["total_size"] > 0
    
    assert "delete_files" in health
    df = health["delete_files"]
    assert df["data_manifest_count"] > 0
    
    assert "column_nulls" in health
    nulls = {c["field_name"]: c["null_pct"] for c in health["column_nulls"]}
    assert "order_id" in nulls
    assert nulls["order_id"] == 0.0  # Should be non-nullable/no nulls

def test_collect_table_health_customers(demo_catalog):
    """Verify health check on a table with schema evolution."""
    tbl = get_table(demo_catalog, "sales.customers")
    health = collect_table_health(tbl)
    
    assert "column_sizes" in health
    sizes = {c["field_name"]: c["total_bytes"] for c in health["column_sizes"]}
    assert "email" in sizes or "email_address" in sizes # Depending on schema version

def test_collect_table_health_empty_table(demo_catalog):
    """Verify health check on a newly created empty table."""
    from pyiceberg.catalog import load_catalog
    
    # Create empty table
    cat = load_catalog(demo_catalog.catalog_name, **demo_catalog.properties)
    try:
        cat.create_namespace_if_not_exists("test_ns")
    except Exception:
        pass
        
    import pyarrow as pa
    schema = pa.schema([pa.field("id", pa.int64())])
    cat.create_table("test_ns.empty", schema=schema)
    
    tbl = get_table(demo_catalog, "test_ns.empty")
    health = collect_table_health(tbl)
    
    assert health["file_health"]["file_count"] == 0
    assert health["file_health"]["total_size"] == 0
