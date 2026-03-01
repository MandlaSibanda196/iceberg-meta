
import os
import random
import time
from datetime import datetime
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

# Configuration
CATALOG_DIR = Path("monster_catalog")
DB_PATH = CATALOG_DIR / "catalog.db"
WAREHOUSE_PATH = CATALOG_DIR / "warehouse"
TABLE_NAME = "bench.monster"

def setup_catalog():
    if not CATALOG_DIR.exists():
        CATALOG_DIR.mkdir()
    if not WAREHOUSE_PATH.exists():
        WAREHOUSE_PATH.mkdir()
    
    return SqlCatalog(
        "monster",
        **{
            "uri": f"sqlite:///{DB_PATH.absolute()}",
            "warehouse": f"file://{WAREHOUSE_PATH.absolute()}",
        }
    )

def create_monster_table(cat):
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, LongType, StringType, TimestampType
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import BucketTransform

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
        NestedField(field_id=3, name="category", field_type=StringType(), required=False),
        NestedField(field_id=4, name="ts", field_type=TimestampType(), required=False),
    )

    try:
        cat.create_namespace_if_not_exists("bench")
    except Exception:
        pass

    try:
        cat.drop_table(TABLE_NAME)
    except Exception:
        pass

    # Bucket partition on ID (1000 buckets)
    # This should create ~1000 files per commit if we write enough random data
    # And since we write random IDs, the 'id' ranges of these files will overlap heavily.
    table = cat.create_table(
        TABLE_NAME,
        schema=schema,
        partition_spec=PartitionSpec(
            PartitionField(source_id=1, field_id=1000, transform=BucketTransform(1000), name="id_bucket")
        ),
        properties={"write.parquet.compression-codec": "zstd"}
    )
    
    # We aim for 5000 files total. 5 commits of 1000 buckets.
    NUM_COMMITS = 5
    BUCKETS = 1000
    
    print(f"Generating ~{NUM_COMMITS * BUCKETS} files via {NUM_COMMITS} commits using Bucket partitioning...")
    print("Files will have overlapping 'id' ranges to stress-test O(N^2) checks.")
    
    start_time = time.time()
    
    pa_schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("data", pa.string()),
        pa.field("category", pa.string()),
        pa.field("ts", pa.timestamp("us")),
    ])

    for i in range(NUM_COMMITS):
        rows = []
        # Generate enough rows to likely fill most buckets
        # With 1000 buckets, 5000 rows might leave empty buckets.
        # Let's use 10,000 rows.
        count = 10000
        for _ in range(count):
            rid = random.randint(0, 1000000)
            rows.append({
                "id": rid,
                "data": f"data-{rid}",
                "category": "cat",
                "ts": datetime.now()
            })
            
        pa_table = pa.Table.from_pylist(rows, schema=pa_schema)
        table.append(pa_table)
        print(f"Commit {i+1}/{NUM_COMMITS}: Written batch ({time.time() - start_time:.1f}s)")

    print(f"Done! Table {TABLE_NAME} created.")
    print(f"Catalog URI: sqlite:///{DB_PATH.absolute()}")
    print(f"Warehouse: {WAREHOUSE_PATH.absolute()}")

if __name__ == "__main__":
    cat = setup_catalog()
    create_monster_table(cat)
