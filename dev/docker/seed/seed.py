"""Seed the catalog with sample Iceberg tables backed by MinIO.

Uses pyiceberg's SQL catalog (SQLite) so that full snapshot history
is preserved in the metadata -- unlike Nessie catalog mode which only
exposes the current snapshot.
"""

import os
import time

from data_factory import seed_catalog
from pyiceberg.catalog.sql import SqlCatalog

CATALOG_PROPS = {
    "uri": os.environ.get("ICEBERG_CATALOG_URI", "sqlite:////catalog/iceberg_catalog.db"),
    "warehouse": os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse"),
    "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://minio:9000"),
    "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "admin"),
    "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "password"),
    "s3.path-style-access": "true",
    "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
}


def wait_for_minio(max_retries=30):
    """Wait until MinIO is ready by trying to initialise the catalog."""
    for i in range(max_retries):
        try:
            catalog = SqlCatalog("local", **CATALOG_PROPS)
            return catalog
        except Exception:
            print(f"Waiting for MinIO... ({i + 1}/{max_retries})")
            time.sleep(3)
    raise RuntimeError("MinIO did not become ready in time.")


def main():
    catalog = wait_for_minio()

    print("Seeding catalog with test data …")
    tables = seed_catalog(catalog)

    print()
    for name in sorted(tables):
        tbl = tables[name]
        snap_count = len(tbl.metadata.snapshots)
        print(f"  {name:<30s} {snap_count} snapshot(s)")

    print(f"\nDone — seeded {len(tables)} tables.")


if __name__ == "__main__":
    main()
