"""Shared test fixtures using SQL catalog backed by MinIO.

Requires:
  - MinIO running (make infra-up)
  - Environment sourced (source dev/.env)
"""

import os
from pathlib import Path

import pytest
import yaml
from pyiceberg.catalog.sql import SqlCatalog

import iceberg_meta.catalog as catalog_module
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

CONFIG_CONTENT = {
    "default_catalog": "local",
    "catalogs": {
        "local": {
            "type": "sql",
            **CATALOG_PROPS,
        },
    },
}


@pytest.fixture(scope="session", autouse=True)
def config_file(tmp_path_factory):
    """Create a temp config file and point the catalog module to it."""
    tmp = tmp_path_factory.mktemp("config")
    config_path = tmp / "iceberg-meta.yaml"
    config_path.write_text(yaml.dump(CONFIG_CONTENT))
    catalog_module.CONFIG_FILE = config_path
    return config_path


@pytest.fixture(scope="session")
def catalog():
    """Seed the full test catalog using data_factory."""
    Path("catalog").mkdir(exist_ok=True)
    cat = SqlCatalog("local", **CATALOG_PROPS)
    seed_catalog(cat)
    return cat


@pytest.fixture(scope="session")
def orders_table(catalog):
    return catalog.load_table("sales.orders")


@pytest.fixture(scope="session")
def sessions_table(catalog):
    return catalog.load_table("analytics.sessions")


@pytest.fixture(scope="session")
def test_table(catalog):
    return catalog.load_table("sales.orders")
