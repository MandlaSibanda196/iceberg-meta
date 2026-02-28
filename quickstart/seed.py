"""Seed sample Iceberg tables for the quickstart.

Reads connection details from environment variables (source .env first).
Creates a few tables with multiple snapshots so you can explore snapshots,
diffs, tree views, and all other iceberg-meta features.
"""

from __future__ import annotations

import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

_RNG = random.Random(42)

CATALOG_PROPS = {
    "uri": os.environ.get("ICEBERG_CATALOG_URI", "sqlite:///catalog/iceberg_catalog.db"),
    "warehouse": os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse"),
    "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
    "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "admin"),
    "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "password"),
    "s3.path-style-access": "true",
    "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
}

NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank"]
REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-south"]
EVENTS = ["page_view", "click", "scroll", "form_submit", "purchase"]
PAGES = ["/home", "/products", "/cart", "/checkout", "/account"]


def _random_ts(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=_RNG.randint(0, int(delta.total_seconds())))


def _seed_orders(cat: SqlCatalog) -> None:
    """sales.orders -- partitioned by region, 4 snapshots."""
    import contextlib
    cat.create_namespace_if_not_exists("sales")
    with contextlib.suppress(Exception):
        cat.drop_table("sales.orders")

    schema = pa.schema([
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("customer_name", pa.string()),
        pa.field("region", pa.string()),
        pa.field("amount", pa.float64()),
        pa.field("order_date", pa.date32()),
        pa.field("created_at", pa.timestamp("us")),
    ])
    table = cat.create_table("sales.orders", schema=schema, properties={
        "owner": "analytics-team",
        "write.format.default": "parquet",
    })

    start, end = datetime(2024, 1, 1), datetime(2024, 6, 30)

    def batch(id_start: int, id_end: int) -> pa.Table:
        rows = []
        for oid in range(id_start, id_end + 1):
            ts = _random_ts(start, end)
            rows.append({
                "order_id": oid,
                "customer_name": _RNG.choice(NAMES),
                "region": _RNG.choice(REGIONS),
                "amount": round(_RNG.uniform(10, 999), 2),
                "order_date": ts.date(),
                "created_at": ts,
            })
        return pa.Table.from_pylist(rows, schema=schema)

    table.append(batch(1, 20))
    table.append(batch(21, 45))
    table.append(batch(46, 60))
    table.overwrite(batch(1, 15))

    t = cat.load_table("sales.orders")
    print(f"  sales.orders          {len(t.metadata.snapshots)} snapshot(s)")


def _seed_events(cat: SqlCatalog) -> None:
    """analytics.events -- 3 append snapshots."""
    import contextlib
    cat.create_namespace_if_not_exists("analytics")
    with contextlib.suppress(Exception):
        cat.drop_table("analytics.events")

    schema = pa.schema([
        pa.field("event_id", pa.int64(), nullable=False),
        pa.field("user_id", pa.int64()),
        pa.field("event_type", pa.string()),
        pa.field("page_url", pa.string()),
        pa.field("duration_ms", pa.int32()),
        pa.field("ts", pa.timestamp("us")),
    ])
    table = cat.create_table("analytics.events", schema=schema)

    eid = 1
    start, end = datetime(2024, 1, 1), datetime(2024, 12, 31)
    for _ in range(3):
        rows = []
        for _ in range(25):
            rows.append({
                "event_id": eid,
                "user_id": _RNG.randint(1000, 1200),
                "event_type": _RNG.choice(EVENTS),
                "page_url": _RNG.choice(PAGES),
                "duration_ms": _RNG.randint(100, 30000),
                "ts": _random_ts(start, end),
            })
            eid += 1
        table.append(pa.Table.from_pylist(rows, schema=schema))

    t = cat.load_table("analytics.events")
    print(f"  analytics.events      {len(t.metadata.snapshots)} snapshot(s)")


def _seed_metrics(cat: SqlCatalog) -> None:
    """staging.metrics -- wide table, single snapshot."""
    import contextlib
    cat.create_namespace_if_not_exists("staging")
    with contextlib.suppress(Exception):
        cat.drop_table("staging.metrics")

    schema = pa.schema([
        pa.field("metric_id", pa.int64(), nullable=False),
        pa.field("host", pa.string()),
        pa.field("service", pa.string()),
        pa.field("cpu_pct", pa.float64()),
        pa.field("mem_pct", pa.float64()),
        pa.field("disk_read_bytes", pa.int64()),
        pa.field("net_rx_bytes", pa.int64()),
        pa.field("error_count", pa.int32()),
        pa.field("request_count", pa.int32()),
        pa.field("is_healthy", pa.bool_()),
        pa.field("collected_at", pa.timestamp("us")),
    ])
    table = cat.create_table("staging.metrics", schema=schema)

    services = ["api-gateway", "auth-svc", "order-svc", "payment-svc"]
    hosts = [f"host-{i:02d}" for i in range(1, 4)]
    start, end = datetime(2024, 11, 1), datetime(2024, 11, 30)

    rows = []
    mid = 1
    for host in hosts:
        for svc in services:
            rows.append({
                "metric_id": mid,
                "host": host,
                "service": svc,
                "cpu_pct": round(_RNG.uniform(5, 95), 2),
                "mem_pct": round(_RNG.uniform(20, 90), 2),
                "disk_read_bytes": _RNG.randint(0, 10**8),
                "net_rx_bytes": _RNG.randint(0, 10**9),
                "error_count": _RNG.randint(0, 100),
                "request_count": _RNG.randint(100, 50000),
                "is_healthy": _RNG.random() > 0.1,
                "collected_at": _random_ts(start, end),
            })
            mid += 1
    table.append(pa.Table.from_pylist(rows, schema=schema))

    t = cat.load_table("staging.metrics")
    print(f"  staging.metrics       {len(t.metadata.snapshots)} snapshot(s)")


def main() -> None:
    Path("catalog").mkdir(exist_ok=True)
    catalog = SqlCatalog("local", **CATALOG_PROPS)

    print("Seeding sample tables ...\n")
    _seed_orders(catalog)
    _seed_events(catalog)
    _seed_metrics(catalog)
    print("\nDone. Try:")
    print("  iceberg-meta list-tables")
    print("  iceberg-meta summary sales.orders")
    print("  iceberg-meta tree analytics.events")


if __name__ == "__main__":
    main()
