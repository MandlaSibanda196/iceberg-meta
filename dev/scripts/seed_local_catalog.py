"""Seed the local SQL catalog (backed by MinIO) with sample Iceberg tables.

Requires:
  - MinIO running (make infra-up)
  - Environment sourced (source dev/.env)
"""

import os
import sys
from pathlib import Path

from pyiceberg.catalog.sql import SqlCatalog

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tests"))
from data_factory import seed_catalog

CATALOG_PROPS = {
    "uri": os.environ.get("ICEBERG_CATALOG_URI", "sqlite:///catalog/iceberg_catalog.db"),
    "warehouse": os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse"),
    "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
    "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "admin"),
    "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "password"),
    "s3.path-style-access": "true",
    "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
}


def main():
    Path("catalog").mkdir(exist_ok=True)
    catalog = SqlCatalog("local", **CATALOG_PROPS)

    print("Seeding catalog with test data …")
    tables = seed_catalog(catalog)

    print()
    for name in sorted(tables):
        tbl = tables[name]
        snap_count = len(tbl.metadata.snapshots)
        print(f"  {name:<30s} {snap_count} snapshot(s)")

    print(f"\nDone — seeded {len(tables)} tables.")
    print()
    print("Try:")
    print("  iceberg-meta table-info sales.orders")
    print("  iceberg-meta tree analytics.events")


if __name__ == "__main__":
    main()
