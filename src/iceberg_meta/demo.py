"""Self-contained demo catalog using SQLite + local filesystem.

Creates a temporary catalog with sample tables so users can explore
iceberg-meta immediately after `pip install` — no Docker, no cloud
credentials, no configuration needed.
"""

from __future__ import annotations

import contextlib
import random
import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.types import DateType, StringType

_RNG = random.Random(42)
_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank"]
_REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-south"]
_EVENTS = ["page_view", "click", "scroll", "form_submit", "purchase"]
_PAGES = ["/home", "/products", "/cart", "/checkout", "/account"]


def _random_ts(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=_RNG.randint(0, int(delta.total_seconds())))


def _seed_orders(cat: SqlCatalog) -> None:
    schema = pa.schema(
        [
            pa.field("order_id", pa.int64(), nullable=False),
            pa.field("customer_name", pa.string()),
            pa.field("region", pa.string()),
            pa.field("amount", pa.float64()),
            pa.field("order_date", pa.date32()),
            pa.field("created_at", pa.timestamp("us")),
        ]
    )
    with contextlib.suppress(Exception):
        cat.drop_table("sales.orders")
    table = cat.create_table(
        "sales.orders",
        schema=schema,
        properties={"owner": "analytics-team", "write.format.default": "parquet"},
    )

    start, end = datetime(2024, 1, 1), datetime(2024, 6, 30)

    def batch(id_start: int, id_end: int) -> pa.Table:
        rows: list[dict[str, Any]] = []
        for oid in range(id_start, id_end + 1):
            ts = _random_ts(start, end)
            rows.append(
                {
                    "order_id": oid,
                    "customer_name": _RNG.choice(_NAMES),
                    "region": _RNG.choice(_REGIONS),
                    "amount": round(_RNG.uniform(10, 999), 2),
                    "order_date": ts.date(),
                    "created_at": ts,
                }
            )
        return pa.Table.from_pylist(rows, schema=schema)

    table.append(batch(1, 20))
    table.append(batch(21, 45))
    table.append(batch(46, 60))
    table.overwrite(batch(1, 15))


def _seed_customers(cat: SqlCatalog) -> None:
    schema_v0 = pa.schema(
        [
            pa.field("customer_id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("email", pa.string()),
        ]
    )
    with contextlib.suppress(Exception):
        cat.drop_table("sales.customers")
    table = cat.create_table("sales.customers", schema=schema_v0)

    table.append(
        pa.table(
            {
                "customer_id": list(range(1, 11)),
                "name": [_RNG.choice(_NAMES) for _ in range(10)],
                "email": [f"user{i}@example.com" for i in range(1, 11)],
            }
        )
    )

    with table.update_schema() as update:
        update.add_column("phone", StringType())
        update.add_column("signup_date", DateType())

    table = cat.load_table("sales.customers")
    table.append(
        pa.table(
            {
                "customer_id": list(range(11, 21)),
                "name": [_RNG.choice(_NAMES) for _ in range(10)],
                "email": [f"user{i}@example.com" for i in range(11, 21)],
                "phone": [f"+1-555-{_RNG.randint(1000, 9999)}" for _ in range(10)],
                "signup_date": [
                    datetime(2024, _RNG.randint(1, 12), _RNG.randint(1, 28)).date()
                    for _ in range(10)
                ],
            }
        )
    )


def _seed_events(cat: SqlCatalog) -> None:
    schema = pa.schema(
        [
            pa.field("event_id", pa.int64(), nullable=False),
            pa.field("user_id", pa.int64()),
            pa.field("event_type", pa.string()),
            pa.field("page_url", pa.string()),
            pa.field("duration_ms", pa.int32()),
            pa.field("ts", pa.timestamp("us")),
        ]
    )
    with contextlib.suppress(Exception):
        cat.drop_table("analytics.events")
    table = cat.create_table("analytics.events", schema=schema)

    eid = 1
    start, end = datetime(2024, 1, 1), datetime(2024, 12, 31)
    for _ in range(3):
        rows: list[dict[str, Any]] = []
        for _ in range(25):
            rows.append(
                {
                    "event_id": eid,
                    "user_id": _RNG.randint(1000, 1200),
                    "event_type": _RNG.choice(_EVENTS),
                    "page_url": _RNG.choice(_PAGES),
                    "duration_ms": _RNG.randint(100, 30000),
                    "ts": _random_ts(start, end),
                }
            )
            eid += 1
        table.append(pa.Table.from_pylist(rows, schema=schema))


def create_demo_catalog(base_dir: Path | None = None) -> tuple[Path, dict[str, str]]:
    """Create a demo catalog with sample tables.

    Returns (directory_path, catalog_properties).
    """
    if base_dir is None:
        base_dir = Path(tempfile.mkdtemp(prefix="iceberg-meta-demo-"))

    db_path = base_dir / "catalog.db"
    warehouse_path = base_dir / "warehouse"
    warehouse_path.mkdir(parents=True, exist_ok=True)

    props = {
        "uri": f"sqlite:///{db_path}",
        "warehouse": str(warehouse_path),
    }

    cat = SqlCatalog("demo", **props)
    _RNG.seed(42)

    for ns in ("sales", "analytics"):
        cat.create_namespace_if_not_exists(ns)

    _seed_orders(cat)
    _seed_customers(cat)
    _seed_events(cat)

    return base_dir, props


def cleanup_demo(demo_dir: Path) -> None:
    """Remove demo directory."""
    shutil.rmtree(demo_dir, ignore_errors=True)
