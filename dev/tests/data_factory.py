"""Deterministic test data factory for Iceberg metadata tests.

Builds a realistic multi-table catalog with varied schemas, partitioning
strategies, snapshot operations, and data volumes.  Both conftest.py and
the seed scripts delegate to ``seed_catalog()`` so that tests and local
dev share identical data.
"""

from __future__ import annotations

import contextlib
import random
from datetime import date, datetime, timedelta
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import LessThanOrEqual
from pyiceberg.io import load_file_io
from pyiceberg.table import Table
from pyiceberg.transforms import BucketTransform
from pyiceberg.types import DateType, StringType

# ---------------------------------------------------------------------------
# Reproducible randomness
# ---------------------------------------------------------------------------

_RNG = random.Random(42)

NAMESPACES = ("sales", "analytics", "staging")

_NAMES = [
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace",
    "Hank", "Ivy", "Jack", "Karen", "Leo", "Mona", "Nick", "Olivia",
    "Paul", "Quinn", "Rita", "Sam", "Tina", "Uma", "Vic", "Wendy",
    "Xander", "Yara", "Zane",
]
_REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-south"]
_BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
_OS_LIST = ["Windows", "macOS", "Linux", "iOS", "Android"]
_EVENT_TYPES = ["page_view", "click", "scroll", "form_submit", "purchase", "logout"]
_PAGES = ["/home", "/products", "/cart", "/checkout", "/account", "/search", "/about"]
_SERVICES = ["api-gateway", "auth-svc", "order-svc", "payment-svc", "notification-svc"]


def _ts(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute)


def _random_ts(start: datetime, end: datetime) -> datetime:
    delta = end - start
    offset = _RNG.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=offset)


def _drop_if_exists(cat: Catalog, identifier: str) -> None:
    with contextlib.suppress(Exception):
        cat.drop_table(identifier)


def _fix_io(table: Table, s3_overrides: dict[str, str] | None) -> None:
    """Override server-returned S3 properties so IO targets the host endpoint.

    REST catalogs (Nessie) return their internal S3 endpoint (e.g.
    ``minio:9000``) which isn't reachable from the host.  This patches
    the table IO with the caller-supplied S3 properties.
    """
    if not s3_overrides:
        return
    merged = dict(table.io.properties)
    merged.update(s3_overrides)
    table.io = load_file_io(properties=merged)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def seed_catalog(
    catalog: Catalog,
    s3_overrides: dict[str, str] | None = None,
) -> dict[str, Table]:
    """Create all namespaces + tables and return ``{identifier: Table}``.

    Parameters
    ----------
    catalog:
        A pyiceberg catalog handle.
    s3_overrides:
        Optional dict of S3 properties (e.g. ``s3.endpoint``) to patch
        onto every table IO.  Required when the REST catalog returns an
        internal endpoint that differs from the host-reachable one.
    """
    for ns in NAMESPACES:
        catalog.create_namespace_if_not_exists(ns)

    _RNG.seed(42)

    tables: dict[str, Table] = {}
    tables["sales.orders"] = _seed_orders(catalog, s3_overrides)
    tables["sales.customers"] = _seed_customers(catalog, s3_overrides)
    tables["analytics.events"] = _seed_events(catalog, s3_overrides)
    tables["analytics.sessions"] = _seed_sessions(catalog, s3_overrides)
    tables["staging.landing_raw"] = _seed_landing_raw(catalog, s3_overrides)
    tables["staging.wide_metrics"] = _seed_wide_metrics(catalog, s3_overrides)
    return tables


# ---------------------------------------------------------------------------
# sales.orders — partitioned, properties, append + overwrite
# ---------------------------------------------------------------------------

ORDERS_SCHEMA = pa.schema([
    pa.field("order_id", pa.int64(), nullable=False),
    pa.field("customer_name", pa.string()),
    pa.field("region", pa.string()),
    pa.field("amount", pa.float64()),
    pa.field("is_priority", pa.bool_()),
    pa.field("order_date", pa.date32()),
    pa.field("created_at", pa.timestamp("us")),
])


def _orders_batch(
    id_start: int, id_end: int, ts_start: datetime, ts_end: datetime,
) -> pa.Table:
    rows: list[dict[str, Any]] = []
    for oid in range(id_start, id_end + 1):
        ts = _random_ts(ts_start, ts_end)
        rows.append({
            "order_id": oid,
            "customer_name": _RNG.choice(_NAMES),
            "region": _RNG.choice(_REGIONS),
            "amount": round(_RNG.uniform(10.0, 999.99), 2),
            "is_priority": _RNG.random() > 0.7,
            "order_date": ts.date(),
            "created_at": ts,
        })
    return pa.Table.from_pylist(rows, schema=ORDERS_SCHEMA)


def _seed_orders(cat: Catalog, s3: dict[str, str] | None) -> Table:
    identifier = "sales.orders"
    _drop_if_exists(cat, identifier)

    table = cat.create_table(
        identifier,
        schema=ORDERS_SCHEMA,
        properties={
            "owner": "analytics-team",
            "write.format.default": "parquet",
            "commit.retry.num-retries": "3",
            "data-classification": "internal",
        },
    )
    _fix_io(table, s3)

    with table.update_spec() as spec_update:
        spec_update.add_identity("region")
    _fix_io(table, s3)

    start, end = _ts(2024, 1, 1), _ts(2024, 4, 30)

    table.append(_orders_batch(1, 15, start, end))        # snapshot 1: 15 rows
    table.append(_orders_batch(16, 35, start, end))       # snapshot 2: 20 rows
    table.append(_orders_batch(36, 50, start, end))       # snapshot 3: 15 rows

    # snapshot 4: overwrite replaces all data with a corrected batch
    table.overwrite(_orders_batch(1, 10, _ts(2024, 5, 1), _ts(2024, 5, 31)))

    result = cat.load_table(identifier)
    _fix_io(result, s3)
    return result


# ---------------------------------------------------------------------------
# sales.customers — schema evolution (add columns, rename)
# ---------------------------------------------------------------------------


def _seed_customers(cat: Catalog, s3: dict[str, str] | None) -> Table:
    identifier = "sales.customers"
    _drop_if_exists(cat, identifier)

    schema_v0 = pa.schema([
        pa.field("customer_id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("email", pa.string()),
    ])

    table = cat.create_table(identifier, schema=schema_v0)
    _fix_io(table, s3)

    # Snapshot 1 — v0 schema
    table.append(pa.table({
        "customer_id": list(range(1, 11)),
        "name": [_RNG.choice(_NAMES) for _ in range(10)],
        "email": [f"user{i}@example.com" for i in range(1, 11)],
    }))

    # Evolution v1: add phone and signup_date
    with table.update_schema() as update:
        update.add_column("phone", StringType())
        update.add_column("signup_date", DateType())
    _fix_io(table, s3)

    # Snapshot 2 — v1 schema
    table.append(pa.table({
        "customer_id": list(range(11, 21)),
        "name": [_RNG.choice(_NAMES) for _ in range(10)],
        "email": [f"user{i}@example.com" for i in range(11, 21)],
        "phone": [f"+1-555-{_RNG.randint(1000, 9999)}" for _ in range(10)],
        "signup_date": [
            date(2024, _RNG.randint(1, 12), _RNG.randint(1, 28)) for _ in range(10)
        ],
    }))

    # Evolution v2: rename email -> email_address
    with table.update_schema() as update:
        update.rename_column("email", "email_address")

    table = cat.load_table(identifier)
    _fix_io(table, s3)

    # Snapshot 3 — v2 schema (use renamed column)
    table.append(pa.table({
        "customer_id": list(range(21, 31)),
        "name": [_RNG.choice(_NAMES) for _ in range(10)],
        "email_address": [f"user{i}@example.com" for i in range(21, 31)],
        "phone": [f"+1-555-{_RNG.randint(1000, 9999)}" for _ in range(10)],
        "signup_date": [
            date(2024, _RNG.randint(1, 12), _RNG.randint(1, 28)) for _ in range(10)
        ],
    }))

    result = cat.load_table(identifier)
    _fix_io(result, s3)
    return result


# ---------------------------------------------------------------------------
# analytics.events — nested struct, bucket partition, many snapshots
# ---------------------------------------------------------------------------

EVENTS_SCHEMA = pa.schema([
    pa.field("event_id", pa.int64(), nullable=False),
    pa.field("user_id", pa.int64()),
    pa.field("event_type", pa.string()),
    pa.field("page_url", pa.string()),
    pa.field("device", pa.struct([
        pa.field("browser", pa.string()),
        pa.field("os", pa.string()),
        pa.field("version", pa.string()),
    ])),
    pa.field("duration_ms", pa.int32()),
    pa.field("is_bounce", pa.bool_()),
    pa.field("ts", pa.timestamp("us")),
])


def _seed_events(cat: Catalog, s3: dict[str, str] | None) -> Table:
    identifier = "analytics.events"
    _drop_if_exists(cat, identifier)

    table = cat.create_table(identifier, schema=EVENTS_SCHEMA)
    _fix_io(table, s3)

    with table.update_spec() as spec_update:
        spec_update.add_field("user_id", BucketTransform(num_buckets=4), "user_id_bucket")
    _fix_io(table, s3)

    eid = 1
    start, end = _ts(2024, 1, 1), _ts(2024, 12, 31)

    for _ in range(5):
        rows: list[dict[str, Any]] = []
        for _ in range(20):
            rows.append({
                "event_id": eid,
                "user_id": _RNG.randint(1000, 1200),
                "event_type": _RNG.choice(_EVENT_TYPES),
                "page_url": _RNG.choice(_PAGES),
                "device": {
                    "browser": _RNG.choice(_BROWSERS),
                    "os": _RNG.choice(_OS_LIST),
                    "version": f"{_RNG.randint(80, 130)}.0.{_RNG.randint(0, 9)}",
                },
                "duration_ms": _RNG.randint(100, 30000),
                "is_bounce": _RNG.random() > 0.6,
                "ts": _random_ts(start, end),
            })
            eid += 1
        table.append(pa.Table.from_pylist(rows, schema=EVENTS_SCHEMA))

    result = cat.load_table(identifier)
    _fix_io(result, s3)
    return result


# ---------------------------------------------------------------------------
# analytics.sessions — append, delete, append
# ---------------------------------------------------------------------------

SESSIONS_SCHEMA = pa.schema([
    pa.field("session_id", pa.int64(), nullable=False),
    pa.field("user_id", pa.int64()),
    pa.field("start_ts", pa.timestamp("us")),
    pa.field("end_ts", pa.timestamp("us")),
    pa.field("page_count", pa.int32()),
    pa.field("region", pa.string()),
])


def _seed_sessions(cat: Catalog, s3: dict[str, str] | None) -> Table:
    identifier = "analytics.sessions"
    _drop_if_exists(cat, identifier)

    table = cat.create_table(identifier, schema=SESSIONS_SCHEMA)
    _fix_io(table, s3)

    start, end = _ts(2024, 6, 1), _ts(2024, 6, 30)

    # Snapshot 1: 20 sessions
    rows: list[dict[str, Any]] = []
    for sid in range(1, 21):
        s = _random_ts(start, end)
        rows.append({
            "session_id": sid,
            "user_id": _RNG.randint(1000, 1200),
            "start_ts": s,
            "end_ts": s + timedelta(minutes=_RNG.randint(1, 120)),
            "page_count": _RNG.randint(1, 25),
            "region": _RNG.choice(_REGIONS),
        })
    table.append(pa.Table.from_pylist(rows, schema=SESSIONS_SCHEMA))

    # Snapshot 2: delete short-visit sessions
    with contextlib.suppress(Exception):
        table.delete(delete_filter=LessThanOrEqual("page_count", 5))

    # Snapshot 3: 10 more sessions (no very-short visits)
    rows = []
    for sid in range(21, 31):
        s = _random_ts(start, end)
        rows.append({
            "session_id": sid,
            "user_id": _RNG.randint(1000, 1200),
            "start_ts": s,
            "end_ts": s + timedelta(minutes=_RNG.randint(5, 120)),
            "page_count": _RNG.randint(6, 25),
            "region": _RNG.choice(_REGIONS),
        })
    table.append(pa.Table.from_pylist(rows, schema=SESSIONS_SCHEMA))

    result = cat.load_table(identifier)
    _fix_io(result, s3)
    return result


# ---------------------------------------------------------------------------
# staging.landing_raw — empty table (no snapshots)
# ---------------------------------------------------------------------------


def _seed_landing_raw(cat: Catalog, s3: dict[str, str] | None) -> Table:
    identifier = "staging.landing_raw"
    _drop_if_exists(cat, identifier)

    schema = pa.schema([
        pa.field("raw_id", pa.int64()),
        pa.field("source", pa.string()),
        pa.field("payload", pa.string()),
        pa.field("ingested_at", pa.timestamp("us")),
    ])

    cat.create_table(identifier, schema=schema)
    result = cat.load_table(identifier)
    _fix_io(result, s3)
    return result


# ---------------------------------------------------------------------------
# staging.wide_metrics — 22 columns, single snapshot
# ---------------------------------------------------------------------------


def _seed_wide_metrics(cat: Catalog, s3: dict[str, str] | None) -> Table:
    identifier = "staging.wide_metrics"
    _drop_if_exists(cat, identifier)

    schema = pa.schema([
        pa.field("metric_id", pa.int64(), nullable=False),
        pa.field("host", pa.string()),
        pa.field("service", pa.string()),
        pa.field("region", pa.string()),
        pa.field("cpu_pct", pa.float64()),
        pa.field("mem_pct", pa.float64()),
        pa.field("disk_read_bytes", pa.int64()),
        pa.field("disk_write_bytes", pa.int64()),
        pa.field("net_rx_bytes", pa.int64()),
        pa.field("net_tx_bytes", pa.int64()),
        pa.field("iops", pa.int32()),
        pa.field("latency_p50_ms", pa.float64()),
        pa.field("latency_p99_ms", pa.float64()),
        pa.field("error_count", pa.int32()),
        pa.field("request_count", pa.int32()),
        pa.field("is_healthy", pa.bool_()),
        pa.field("uptime_seconds", pa.int64()),
        pa.field("gc_pause_ms", pa.float64()),
        pa.field("thread_count", pa.int32()),
        pa.field("open_fds", pa.int32()),
        pa.field("heap_used_mb", pa.float64()),
        pa.field("collected_at", pa.timestamp("us")),
    ])

    table = cat.create_table(identifier, schema=schema)
    _fix_io(table, s3)

    hosts = [f"host-{i:02d}" for i in range(1, 6)]
    ts_start, ts_end = _ts(2024, 11, 1), _ts(2024, 11, 30)

    rows: list[dict[str, Any]] = []
    mid = 1
    for host in hosts:
        for svc in _SERVICES:
            rows.append({
                "metric_id": mid,
                "host": host,
                "service": svc,
                "region": _RNG.choice(_REGIONS),
                "cpu_pct": round(_RNG.uniform(5.0, 95.0), 2),
                "mem_pct": round(_RNG.uniform(20.0, 90.0), 2),
                "disk_read_bytes": _RNG.randint(0, 10**8),
                "disk_write_bytes": _RNG.randint(0, 10**8),
                "net_rx_bytes": _RNG.randint(0, 10**9),
                "net_tx_bytes": _RNG.randint(0, 10**9),
                "iops": _RNG.randint(50, 5000),
                "latency_p50_ms": round(_RNG.uniform(0.5, 50.0), 2),
                "latency_p99_ms": round(_RNG.uniform(10.0, 500.0), 2),
                "error_count": _RNG.randint(0, 100),
                "request_count": _RNG.randint(100, 50000),
                "is_healthy": _RNG.random() > 0.1,
                "uptime_seconds": _RNG.randint(3600, 2_592_000),
                "gc_pause_ms": round(_RNG.uniform(0.1, 200.0), 2),
                "thread_count": _RNG.randint(10, 500),
                "open_fds": _RNG.randint(50, 2000),
                "heap_used_mb": round(_RNG.uniform(64.0, 2048.0), 2),
                "collected_at": _random_ts(ts_start, ts_end),
            })
            mid += 1

    table.append(pa.Table.from_pylist(rows, schema=schema))
    result = cat.load_table(identifier)
    _fix_io(result, s3)
    return result
