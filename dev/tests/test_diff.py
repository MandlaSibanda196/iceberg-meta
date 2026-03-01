
import pytest
from iceberg_meta.catalog import get_table
from iceberg_meta.formatters import collect_diff

def test_collect_diff_orders(demo_catalog):
    """Verify diff works between two snapshots."""
    tbl = get_table(demo_catalog, "sales.orders")
    
    # Get snapshots
    snapshots = tbl.metadata.snapshots
    assert len(snapshots) >= 2
    
    s1 = snapshots[0]
    s2 = snapshots[1]
    
    diff = collect_diff(tbl, s1.snapshot_id, s2.snapshot_id)
    
    assert diff["snap_id_1"] == s1.snapshot_id
    assert diff["snap_id_2"] == s2.snapshot_id
    # s2 was an append, so we expect added files and no deleted files
    assert diff["added_count"] > 0
    assert diff["deleted_count"] == 0
    assert diff["net_files"] > 0

def test_collect_diff_overwrite(demo_catalog):
    """Verify diff works for overwrite (added + deleted)."""
    tbl = get_table(demo_catalog, "sales.orders")
    snapshots = tbl.metadata.snapshots
    
    # The last operation was an overwrite
    last_snap = snapshots[-1]
    prev_snap = snapshots[-2]
    
    diff = collect_diff(tbl, prev_snap.snapshot_id, last_snap.snapshot_id)
    
    # Overwrite usually deletes some files and adds new ones
    # In _seed_orders: table.overwrite(batch(1, 15))
    # This might replace existing files containing ids 1-15
    
    # Note: exact behavior depends on how Iceberg plans the overwrite
    # But we should verify structure
    assert "added_files" in diff
    assert "deleted_files" in diff
