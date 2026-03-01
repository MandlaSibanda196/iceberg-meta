"""Catalog connection and configuration management."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pyiceberg.catalog import load_catalog
from pyiceberg.io import load_file_io
from pyiceberg.table import Table

CONFIG_FILE = Path.home() / ".iceberg-meta.yaml"

S3_PROPERTY_KEYS = [
    "s3.endpoint",
    "s3.access-key-id",
    "s3.secret-access-key",
    "s3.path-style-access",
    "s3.region",
]

ENV_VAR_MAP: dict[str, str] = {
    "ICEBERG_META_CATALOG_URI": "uri",
    "ICEBERG_META_WAREHOUSE": "warehouse",
    "ICEBERG_META_S3_ENDPOINT": "s3.endpoint",
    "ICEBERG_META_S3_ACCESS_KEY": "s3.access-key-id",
    "ICEBERG_META_S3_SECRET_KEY": "s3.secret-access-key",
    "ICEBERG_META_S3_REGION": "s3.region",
}


@dataclass
class CatalogConfig:
    """Resolved catalog configuration."""

    catalog_name: str
    properties: dict[str, str]


def load_config_file() -> dict[str, Any]:
    """Load ~/.iceberg-meta.yaml if it exists."""
    if not CONFIG_FILE.exists():
        return {}
    try:
        with open(CONFIG_FILE) as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as exc:
        raise ValueError(f"Config file {CONFIG_FILE} contains invalid YAML:\n  {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(
            f"Config file {CONFIG_FILE} must be a YAML mapping, got {type(data).__name__}"
        )
    return data


def _resolve_placeholders(value: str) -> str:
    """Expand ``${VAR_NAME}`` tokens in *value* using the environment."""

    def _replacer(match: re.Match[str]) -> str:
        var = match.group(1)
        env_val = os.environ.get(var)
        if env_val is None:
            raise ValueError(f"Environment variable ${{{var}}} referenced in config but not set")
        return env_val

    return re.sub(r"\$\{(\w+)\}", _replacer, value)


def _resolve_props(props: dict[str, str]) -> dict[str, str]:
    """Resolve ``${VAR}`` placeholders and coerce all values to strings.

    YAML may parse bare ``true``/``false`` as Python bools and numbers as
    ints/floats.  pyiceberg expects all property values to be strings.
    """

    def _to_str(v: Any) -> str:
        if isinstance(v, bool):
            return str(v).lower()
        if isinstance(v, str):
            return _resolve_placeholders(v)
        return str(v)

    return {k: _to_str(v) for k, v in props.items()}


def _apply_env_overrides(props: dict[str, str]) -> dict[str, str]:
    """Overlay environment variables onto catalog properties."""
    for env_key, prop_key in ENV_VAR_MAP.items():
        value = os.environ.get(env_key)
        if value:
            props[prop_key] = value
    return props


def resolve_catalog_config(
    catalog_name: str | None = None,
    catalog_uri: str | None = None,
    warehouse: str | None = None,
) -> CatalogConfig:
    """Resolve catalog config from CLI flags, config file, or pyiceberg native config.

    Priority: CLI flags > env vars > ~/.iceberg-meta.yaml > pyiceberg native config.
    """
    if catalog_uri:
        props: dict[str, str] = {"uri": catalog_uri}
        if warehouse:
            props["warehouse"] = warehouse
        props = _apply_env_overrides(props)
        return CatalogConfig(
            catalog_name=catalog_name or "default",
            properties=props,
        )

    file_config = load_config_file()
    if file_config:
        name = catalog_name or file_config.get("default_catalog", "default")
        catalogs = file_config.get("catalogs", {})
        if name in catalogs:
            props = dict(catalogs[name])
            props = _resolve_props(props)
            props = _apply_env_overrides(props)
            return CatalogConfig(catalog_name=name, properties=props)
        if catalog_name:
            available = ", ".join(sorted(catalogs)) if catalogs else "(none)"
            raise ValueError(
                f"Catalog '{catalog_name}' not found in {CONFIG_FILE}. "
                f"Available catalogs: {available}"
            )
        if name != "default" and catalogs:
            available = ", ".join(sorted(catalogs))
            raise ValueError(
                f"Default catalog '{name}' not found in {CONFIG_FILE}. "
                f"Available catalogs: {available}"
            )

    props = _apply_env_overrides({})
    return CatalogConfig(
        catalog_name=catalog_name or "default",
        properties=props,
    )


def get_table(config: CatalogConfig, table_identifier: str) -> Table:
    """Load an Iceberg table by its fully-qualified name (namespace.table).

    When client-side S3 properties are provided, they override the server-returned
    IO properties. This is needed when the REST catalog (e.g. Nessie) returns an
    internal S3 endpoint (like http://minio:9000) that isn't reachable from the host.
    """
    catalog = load_catalog(config.catalog_name, **config.properties)
    table = catalog.load_table(table_identifier)

    s3_overrides = {k: v for k, v in config.properties.items() if k in S3_PROPERTY_KEYS}
    if s3_overrides:
        merged = dict(table.io.properties)
        merged.update(s3_overrides)
        table.io = load_file_io(properties=merged)

    return table


def list_all_tables(config: CatalogConfig) -> dict[str, list[str]]:
    """Return ``{namespace: [table_name, ...]}`` for every namespace in the catalog.

    Recursively discovers nested namespaces so that tables in sub-namespaces
    (e.g. ``analytics.staging.temp``) are included.
    """
    catalog = load_catalog(config.catalog_name, **config.properties)
    result: dict[str, list[str]] = {}
    visited: set[tuple[str, ...]] = set()

    def _walk(parent: tuple[str, ...] = ()) -> None:
        try:
            namespaces = catalog.list_namespaces(parent) if parent else catalog.list_namespaces()
        except Exception:
            return
        for ns in namespaces:
            if ns in visited:
                continue
            visited.add(ns)
            ns_str = ".".join(ns)
            tables = catalog.list_tables(ns)
            result[ns_str] = [".".join(t) for t in tables]
            _walk(ns)

    _walk()
    return result


def write_config_file(config: dict[str, Any]) -> Path:
    """Write catalog config to ~/.iceberg-meta.yaml and return the path."""
    try:
        with open(CONFIG_FILE, "w") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    except PermissionError:
        raise ValueError(
            f"Permission denied writing to {CONFIG_FILE}. Check file/directory permissions."
        ) from None
    except OSError as exc:
        raise ValueError(f"Could not write config to {CONFIG_FILE}: {exc}") from None
    return CONFIG_FILE


def merge_config_file(
    catalog_name: str,
    props: dict[str, str],
    *,
    make_default: bool = False,
) -> Path:
    """Add or update a single catalog in ~/.iceberg-meta.yaml without touching others."""
    existing = load_config_file()
    catalogs = existing.get("catalogs", {})
    catalogs[catalog_name] = props
    existing["catalogs"] = catalogs
    if make_default or "default_catalog" not in existing:
        existing["default_catalog"] = catalog_name
    return write_config_file(existing)


def test_connection(config: CatalogConfig) -> dict[str, Any]:
    """Verify catalog connectivity. Returns namespace/table counts or raises."""
    catalog = load_catalog(config.catalog_name, **config.properties)
    namespaces = catalog.list_namespaces()
    table_count = 0
    for ns in namespaces:
        table_count += len(catalog.list_tables(ns))
    return {
        "namespace_count": len(namespaces),
        "table_count": table_count,
    }
