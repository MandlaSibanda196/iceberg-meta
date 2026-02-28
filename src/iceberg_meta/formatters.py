"""Rich formatters for Iceberg metadata display."""

from __future__ import annotations

import logging
import statistics
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table as RichTable
from rich.tree import Tree

from iceberg_meta.output import OutputFormat, emit
from iceberg_meta.utils import format_bytes, format_timestamp_ms, truncate_path

if TYPE_CHECKING:
    from pyiceberg.schema import Schema
    from pyiceberg.table import Table as IcebergTable

    from iceberg_meta.catalog import CatalogConfig

log = logging.getLogger(__name__)


# ─── helpers ──────────────────────────────────────────────────


def _find_snapshot(tbl: IcebergTable, snapshot_id: int):
    """Find a snapshot by ID in the table metadata."""
    for snap in tbl.metadata.snapshots:
        if snap.snapshot_id == snapshot_id:
            return snap
    return None


def _size_color(size_bytes: int, avg_bytes: float) -> str:
    """Return a Rich color tag based on file size relative to the average."""
    if avg_bytes == 0:
        return "white"
    ratio = size_bytes / avg_bytes
    if ratio <= 0.5:
        return "green"
    elif ratio <= 1.5:
        return "yellow"
    return "red"


# ─── table-info ───────────────────────────────────────────────


def collect_table_info(tbl: IcebergTable) -> list[dict[str, Any]]:
    """Extract table overview as a list of key-value dicts."""
    md = tbl.metadata
    return [
        {"Key": "Format Version", "Value": str(md.format_version)},
        {"Key": "Table UUID", "Value": str(md.table_uuid)},
        {"Key": "Location", "Value": md.location},
        {"Key": "Current Schema ID", "Value": str(md.current_schema_id)},
        {"Key": "Default Spec ID", "Value": str(md.default_spec_id)},
        {"Key": "Last Updated", "Value": format_timestamp_ms(md.last_updated_ms)},
        {"Key": "Snapshots", "Value": str(len(md.snapshots))},
        {"Key": "Current Snapshot", "Value": str(md.current_snapshot_id or "None")},
    ]


def render_table_info(
    console: Console, tbl: IcebergTable, fmt: OutputFormat = OutputFormat.TABLE
) -> None:
    """Render table overview."""
    data = collect_table_info(tbl)

    if fmt != OutputFormat.TABLE:
        emit(console, data, ["Key", "Value"], fmt, title="Table Info")
        return

    md = tbl.metadata
    info = RichTable(show_header=False, box=None, padding=(0, 2))
    info.add_column("Key", style="bold cyan")
    info.add_column("Value")
    for row in data:
        info.add_row(row["Key"], row["Value"])

    console.print(Panel(info, title="[bold]Table Info[/bold]", border_style="blue"))

    if md.properties:
        props = RichTable(title="Properties", show_lines=True)
        props.add_column("Key", style="cyan")
        props.add_column("Value")
        for key, value in sorted(md.properties.items()):
            props.add_row(key, value)
        console.print(props)

    render_schema(console, tbl.schema())

    if md.default_spec_id < len(md.partition_specs):
        spec = md.partition_specs[md.default_spec_id]
        if spec.fields:
            console.print("\n[bold]Partition Spec:[/bold]")
            for field in spec.fields:
                console.print(f"  {field.name} = {field.transform}(source_id={field.source_id})")
        else:
            console.print("\n[bold]Partition Spec:[/bold] unpartitioned")


# ─── schema ───────────────────────────────────────────────────


def render_schema(console: Console, schema: Schema) -> None:
    """Render a schema as a Rich Tree."""
    tree = Tree(f"[bold]Schema (id={schema.schema_id})[/bold]")
    for field in schema.fields:
        _add_field_to_tree(tree, field)
    console.print(tree)


def _add_field_to_tree(parent: Tree, field) -> None:
    """Recursively add a field to the tree."""
    required = "required" if field.required else "optional"
    label = (
        f"[cyan]{field.name}[/cyan] : [yellow]{field.field_type}[/yellow]"
        f" ({required}, id={field.field_id})"
    )

    if hasattr(field.field_type, "fields"):
        branch = parent.add(label)
        for sub in field.field_type.fields:
            _add_field_to_tree(branch, sub)
    else:
        parent.add(label)


def render_schema_history(console: Console, tbl: IcebergTable) -> None:
    """Render all historical schemas."""
    schemas = tbl.schemas()
    current_id = tbl.metadata.current_schema_id
    for schema_id in sorted(schemas):
        suffix = " [bold green](current)[/bold green]" if schema_id == current_id else ""
        console.print(f"\n[bold]Schema {schema_id}{suffix}[/bold]")
        render_schema(console, schemas[schema_id])


# ─── snapshots ────────────────────────────────────────────────


def collect_snapshots(tbl: IcebergTable) -> list[dict[str, Any]]:
    """Extract snapshot rows as dicts."""
    rows: list[dict[str, Any]] = []
    for snap in tbl.metadata.snapshots:
        summary_parts = []
        if snap.summary:
            for k, v in snap.summary.additional_properties.items():
                summary_parts.append(f"{k}={v}")
        rows.append(
            {
                "Snapshot ID": str(snap.snapshot_id),
                "Timestamp": format_timestamp_ms(snap.timestamp_ms),
                "Parent ID": str(snap.parent_snapshot_id or "-"),
                "Operation": snap.summary.operation.value if snap.summary else "-",
                "Summary": ", ".join(summary_parts) if summary_parts else "-",
            }
        )
    return rows


def render_snapshots(
    console: Console, tbl: IcebergTable, fmt: OutputFormat = OutputFormat.TABLE
) -> None:
    """Render snapshots."""
    data = collect_snapshots(tbl)
    columns = ["Snapshot ID", "Timestamp", "Parent ID", "Operation", "Summary"]
    styles: dict[str, dict[str, Any]] = {
        "Snapshot ID": {"style": "bold"},
        "Timestamp": {"style": "green"},
        "Operation": {"style": "yellow"},
    }
    emit(console, data, columns, fmt, title="Snapshots", column_styles=styles)


# ─── manifests ────────────────────────────────────────────────


def collect_manifests(tbl: IcebergTable, snapshot_id: int | None = None) -> list[dict[str, Any]]:
    """Extract manifest rows for the current or specified snapshot."""
    snap = None
    if snapshot_id:
        snap = _find_snapshot(tbl, snapshot_id)
    else:
        if tbl.metadata.current_snapshot_id is not None:
            snap = _find_snapshot(tbl, tbl.metadata.current_snapshot_id)
    if not snap:
        return []

    rows: list[dict[str, Any]] = []
    try:
        manifest_list = snap.manifests(tbl.io)
    except Exception:
        log.debug("Failed to read manifests for snapshot %s", snap.snapshot_id, exc_info=True)
        return rows

    for m in manifest_list:
        rows.append(
            {
                "Path": truncate_path(m.manifest_path),
                "Length": format_bytes(m.manifest_length),
                "Spec ID": str(m.partition_spec_id),
                "Content": str(m.content.value) if m.content else "-",
                "Added": str(m.added_files_count or 0),
                "Existing": str(m.existing_files_count or 0),
                "Deleted": str(m.deleted_files_count or 0),
            }
        )
    return rows


def render_manifests(
    console: Console,
    tbl: IcebergTable,
    snapshot_id: int | None = None,
    fmt: OutputFormat = OutputFormat.TABLE,
) -> None:
    """Render manifest files for current or specified snapshot."""
    snap = None
    if snapshot_id:
        snap = _find_snapshot(tbl, snapshot_id)
        if not snap:
            console.print(f"[red]Snapshot {snapshot_id} not found[/red]")
            return
    else:
        if tbl.metadata.current_snapshot_id is None:
            console.print("[yellow]Table has no snapshots[/yellow]")
            return
        snap = _find_snapshot(tbl, tbl.metadata.current_snapshot_id)

    data = collect_manifests(tbl, snapshot_id=snapshot_id)
    columns = ["Path", "Length", "Spec ID", "Content", "Added", "Existing", "Deleted"]
    styles: dict[str, dict[str, Any]] = {
        "Path": {"style": "cyan", "max_width": 60},
        "Length": {"justify": "right"},
        "Content": {"style": "yellow"},
        "Added": {"justify": "right", "style": "green"},
        "Existing": {"justify": "right"},
        "Deleted": {"justify": "right", "style": "red"},
    }

    if fmt == OutputFormat.TABLE:
        console.print(f"[bold]Manifests for snapshot {snap.snapshot_id}[/bold]")
        console.print(f"Manifest list: [cyan]{truncate_path(snap.manifest_list)}[/cyan]\n")

    emit(console, data, columns, fmt, title="Manifest Files", column_styles=styles)


# ─── files ────────────────────────────────────────────────────


def collect_files(tbl: IcebergTable, snapshot_id: int | None = None) -> list[dict[str, Any]]:
    """Extract data file rows."""
    kwargs = {"snapshot_id": snapshot_id} if snapshot_id else {}
    try:
        pa_table = tbl.inspect.files(**kwargs)
    except Exception:
        log.debug("Failed to inspect files for table %s", tbl.name(), exc_info=True)
        return []

    rows: list[dict[str, Any]] = []
    for row in pa_table.to_pylist():
        rows.append(
            {
                "File Path": truncate_path(row.get("file_path", "")),
                "Format": str(row.get("file_format", "")),
                "Record Count": f"{row.get('record_count', 0):,}",
                "File Size": format_bytes(row.get("file_size_in_bytes", 0)),
                "file_size_raw": row.get("file_size_in_bytes", 0),
                "record_count_raw": row.get("record_count", 0),
            }
        )
    return rows


def render_files(
    console: Console,
    tbl: IcebergTable,
    snapshot_id: int | None = None,
    fmt: OutputFormat = OutputFormat.TABLE,
) -> None:
    """Render data file listing with sizes and row counts."""
    data = collect_files(tbl, snapshot_id=snapshot_id)
    columns = ["File Path", "Format", "Record Count", "File Size"]
    styles: dict[str, dict[str, Any]] = {
        "File Path": {"style": "cyan", "max_width": 50},
        "Format": {"style": "yellow"},
        "Record Count": {"justify": "right", "style": "green"},
        "File Size": {"justify": "right"},
    }
    emit(console, data, columns, fmt, title="Data Files", column_styles=styles)
    if fmt == OutputFormat.TABLE:
        console.print(f"\n[dim]Total files: {len(data)}[/dim]")


# ─── partitions ───────────────────────────────────────────────


def collect_partitions(tbl: IcebergTable) -> list[dict[str, Any]]:
    """Extract partition statistics rows."""
    pa_table = tbl.inspect.partitions()
    rows: list[dict[str, Any]] = []
    for row in pa_table.to_pylist():
        rows.append(
            {
                "Partition": str(row.get("partition", "")),
                "Record Count": f"{row.get('record_count', 0):,}",
                "File Count": str(row.get("file_count", 0)),
                "Total Size": format_bytes(row.get("total_data_file_size_in_bytes", 0)),
            }
        )
    return rows


def render_partitions(
    console: Console, tbl: IcebergTable, fmt: OutputFormat = OutputFormat.TABLE
) -> None:
    """Render partition statistics."""
    data = collect_partitions(tbl)
    columns = ["Partition", "Record Count", "File Count", "Total Size"]
    styles: dict[str, dict[str, Any]] = {
        "Partition": {"style": "cyan"},
        "Record Count": {"justify": "right", "style": "green"},
        "File Count": {"justify": "right"},
        "Total Size": {"justify": "right"},
    }
    emit(console, data, columns, fmt, title="Partition Statistics", column_styles=styles)


# ─── table health ─────────────────────────────────────────────

SMALL_FILE_THRESHOLD = 32 * 1024 * 1024  # 32 MB


def _to_dict(value: Any) -> dict:
    """Normalize a PyArrow map column value to a Python dict.

    PyArrow's to_pylist() may return map columns as a list of
    {'key': k, 'value': v} dicts instead of a plain dict.
    """
    if isinstance(value, dict):
        return value
    if isinstance(value, list):
        result = {}
        for item in value:
            if isinstance(item, dict) and "key" in item and "value" in item:
                result[item["key"]] = item["value"]
            elif isinstance(item, (list, tuple)) and len(item) == 2:
                result[item[0]] = item[1]
        return result
    return {}


def _format_bound_value(value: Any) -> str:
    """Format a deserialized bound value for display."""
    if isinstance(value, float):
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    s = str(value)
    if len(s) > 40:
        return s[:37] + "..."
    return s


def collect_table_health(tbl: IcebergTable) -> dict[str, Any]:
    """Compute comprehensive health metrics for a table.

    Returns a dict with sections: file_health, delete_files, partitions,
    column_nulls, column_sizes, column_bounds, partition_overlap.
    """
    result: dict[str, Any] = {}

    # --- file health --------------------------------------------------
    try:
        pa_files = tbl.inspect.files()
        file_rows = pa_files.to_pylist()
    except Exception:
        log.debug("Failed to inspect files for health", exc_info=True)
        file_rows = []

    sizes = [r.get("file_size_in_bytes", 0) for r in file_rows]
    if sizes:
        small = [s for s in sizes if s < SMALL_FILE_THRESHOLD]
        result["file_health"] = {
            "file_count": len(sizes),
            "min_size": min(sizes),
            "max_size": max(sizes),
            "avg_size": int(statistics.mean(sizes)),
            "median_size": int(statistics.median(sizes)),
            "total_size": sum(sizes),
            "small_file_count": len(small),
            "small_file_warning": len(small) > 0,
        }
    else:
        result["file_health"] = {
            "file_count": 0,
            "min_size": 0,
            "max_size": 0,
            "avg_size": 0,
            "median_size": 0,
            "total_size": 0,
            "small_file_count": 0,
            "small_file_warning": False,
        }

    # --- delete file accumulation -------------------------------------
    data_manifests = 0
    delete_manifests = 0
    try:
        if tbl.metadata.current_snapshot_id is not None:
            snap = _find_snapshot(tbl, tbl.metadata.current_snapshot_id)
            if snap:
                for m in snap.manifests(tbl.io):
                    content = str(m.content.value) if m.content else "0"
                    if content == "0":
                        data_manifests += 1
                    else:
                        delete_manifests += 1
    except Exception:
        log.debug("Failed to read manifests for health", exc_info=True)

    result["delete_files"] = {
        "data_manifest_count": data_manifests,
        "delete_manifest_count": delete_manifests,
        "compaction_recommended": delete_manifests > 0,
    }

    # --- partition spec (how the table is partitioned) ------------------
    partition_spec: list[dict[str, str]] = []
    try:
        md = tbl.metadata
        schema = tbl.schema()
        field_by_id_spec = {f.field_id: f for f in schema.fields}
        if md.default_spec_id < len(md.partition_specs):
            spec = md.partition_specs[md.default_spec_id]
            for pf in spec.fields:
                source_field = field_by_id_spec.get(pf.source_id)
                source_name = source_field.name if source_field else f"id={pf.source_id}"
                partition_spec.append(
                    {
                        "name": pf.name,
                        "transform": str(pf.transform),
                        "source_field": source_name,
                    }
                )
    except Exception:
        log.debug("Failed to read partition spec for health", exc_info=True)
    result["partition_spec"] = partition_spec

    # --- partition distribution with skew detection -------------------
    try:
        part_rows = tbl.inspect.partitions().to_pylist()
    except Exception:
        log.debug("Failed to inspect partitions for health", exc_info=True)
        part_rows = []

    partition_data: list[dict[str, Any]] = []
    file_counts: list[int] = []
    for row in part_rows:
        fc = row.get("file_count", 0)
        file_counts.append(fc)
        partition_data.append(
            {
                "partition": str(row.get("partition", "")),
                "record_count": row.get("record_count", 0),
                "file_count": fc,
                "total_size": row.get("total_data_file_size_in_bytes", 0),
            }
        )

    avg_fc = statistics.mean(file_counts) if file_counts else 0
    skewed: list[str] = []
    for p in partition_data:
        if avg_fc > 0 and p["file_count"] > avg_fc * 2:
            p["skewed"] = True
            skewed.append(p["partition"])
        else:
            p["skewed"] = False

    result["partitions"] = {
        "rows": partition_data,
        "count": len(partition_data),
        "skew_ratio": (max(file_counts) / avg_fc) if avg_fc else 0,
        "skewed_partitions": skewed,
    }

    # --- column-level stats from inspect.files rows -------------------
    schema = tbl.schema()
    field_by_id: dict[int, Any] = {}
    for field in schema.fields:
        field_by_id[field.field_id] = field

    null_counts: dict[int, int] = defaultdict(int)
    value_counts: dict[int, int] = defaultdict(int)
    col_sizes: dict[int, int] = defaultdict(int)
    lower_bounds_raw: dict[int, list[bytes]] = defaultdict(list)
    upper_bounds_raw: dict[int, list[bytes]] = defaultdict(list)

    for row in file_rows:
        for fid_key, cnt in _to_dict(row.get("null_value_counts")).items():
            null_counts[int(fid_key)] += cnt
        for fid_key, cnt in _to_dict(row.get("value_counts")).items():
            value_counts[int(fid_key)] += cnt
        for fid_key, sz in _to_dict(row.get("column_sizes")).items():
            col_sizes[int(fid_key)] += sz
        for fid_key, val in _to_dict(row.get("lower_bounds")).items():
            if val is not None:
                lower_bounds_raw[int(fid_key)].append(val)
        for fid_key, val in _to_dict(row.get("upper_bounds")).items():
            if val is not None:
                upper_bounds_raw[int(fid_key)].append(val)

    # column null rates
    column_nulls: list[dict[str, Any]] = []
    for fid in sorted(field_by_id.keys()):
        if fid not in value_counts:
            continue
        field = field_by_id[fid]
        total = value_counts[fid]
        nulls = null_counts.get(fid, 0)
        pct = (nulls / total * 100) if total > 0 else 0.0
        column_nulls.append(
            {
                "field_name": field.name,
                "null_count": nulls,
                "total_count": total,
                "null_pct": pct,
            }
        )
    result["column_nulls"] = column_nulls

    # column sizes
    total_col_size = sum(col_sizes.values()) or 1
    column_sizes_list: list[dict[str, Any]] = []
    for fid, sz in sorted(col_sizes.items(), key=lambda x: x[1], reverse=True):
        if fid in field_by_id:
            column_sizes_list.append(
                {
                    "field_name": field_by_id[fid].name,
                    "total_bytes": sz,
                    "pct_of_total": sz / total_col_size * 100,
                }
            )
    result["column_sizes"] = column_sizes_list

    # column bounds
    column_bounds: list[dict[str, Any]] = []
    try:
        from pyiceberg.conversions import from_bytes
        from pyiceberg.types import PrimitiveType

        for fid in sorted(field_by_id.keys()):
            if fid not in lower_bounds_raw and fid not in upper_bounds_raw:
                continue
            field = field_by_id[fid]
            if not isinstance(field.field_type, PrimitiveType):
                continue
            try:
                lowers = lower_bounds_raw.get(fid, [])
                uppers = upper_bounds_raw.get(fid, [])
                decoded_lowers = [from_bytes(field.field_type, b) for b in lowers if b]
                decoded_uppers = [from_bytes(field.field_type, b) for b in uppers if b]
                min_val = min(decoded_lowers) if decoded_lowers else None
                max_val = max(decoded_uppers) if decoded_uppers else None
                if min_val is not None or max_val is not None:
                    column_bounds.append(
                        {
                            "field_name": field.name,
                            "min_value": _format_bound_value(min_val)
                            if min_val is not None
                            else "?",
                            "max_value": _format_bound_value(max_val)
                            if max_val is not None
                            else "?",
                        }
                    )
            except Exception:
                log.debug("Failed to decode bounds for field %s", field.name, exc_info=True)
    except ImportError:
        log.debug("pyiceberg.conversions not available, skipping bounds")
    result["column_bounds"] = column_bounds

    # --- partition overlap detection ----------------------------------
    overlap_count = 0
    overlap_warning = False
    try:
        specs = tbl.metadata.partition_specs
        default_spec_id = tbl.metadata.default_spec_id
        if default_spec_id < len(specs):
            spec = specs[default_spec_id]
            if spec.fields:
                source_id = spec.fields[0].source_id
                if source_id in field_by_id:
                    field = field_by_id[source_id]
                    from pyiceberg.types import PrimitiveType as PT

                    if isinstance(field.field_type, PT):
                        from pyiceberg.conversions import from_bytes as fb

                        file_ranges: list[tuple[Any, Any]] = []
                        for row in file_rows:
                            lb = _to_dict(row.get("lower_bounds")).get(source_id)
                            ub = _to_dict(row.get("upper_bounds")).get(source_id)
                            if lb is not None and ub is not None:
                                try:
                                    lo = fb(field.field_type, lb)
                                    hi = fb(field.field_type, ub)
                                    file_ranges.append((lo, hi))
                                except Exception:
                                    pass
                        for i in range(len(file_ranges)):
                            for j in range(i + 1, len(file_ranges)):
                                lo_i, hi_i = file_ranges[i]
                                lo_j, hi_j = file_ranges[j]
                                if lo_i <= hi_j and lo_j <= hi_i:
                                    overlap_count += 1
                        overlap_warning = overlap_count > 0
    except Exception:
        log.debug("Failed to compute partition overlap", exc_info=True)

    result["partition_overlap"] = {
        "overlapping_pairs": overlap_count,
        "overlap_warning": overlap_warning,
    }

    return result


def render_table_health(
    console: Console, tbl: IcebergTable, fmt: OutputFormat = OutputFormat.TABLE
) -> None:
    """Render comprehensive table health report."""
    health = collect_table_health(tbl)

    if fmt != OutputFormat.TABLE:
        flat: list[dict[str, Any]] = []
        fh = health["file_health"]
        flat.append(
            {
                "Section": "File Health",
                "Metric": "file_count",
                "Value": str(fh["file_count"]),
            }
        )
        flat.append(
            {
                "Section": "File Health",
                "Metric": "total_size",
                "Value": format_bytes(fh["total_size"]),
            }
        )
        flat.append(
            {
                "Section": "File Health",
                "Metric": "avg_size",
                "Value": format_bytes(fh["avg_size"]),
            }
        )
        flat.append(
            {
                "Section": "File Health",
                "Metric": "small_files",
                "Value": str(fh["small_file_count"]),
            }
        )
        df = health["delete_files"]
        flat.append(
            {
                "Section": "Delete Files",
                "Metric": "data_manifests",
                "Value": str(df["data_manifest_count"]),
            }
        )
        flat.append(
            {
                "Section": "Delete Files",
                "Metric": "delete_manifests",
                "Value": str(df["delete_manifest_count"]),
            }
        )
        flat.append(
            {
                "Section": "Delete Files",
                "Metric": "compaction_recommended",
                "Value": str(df["compaction_recommended"]),
            }
        )
        for p in health["partitions"]["rows"]:
            flat.append(
                {
                    "Section": "Partitions",
                    "Metric": p["partition"],
                    "Value": f"{p['file_count']} files, {p['record_count']:,} rows",
                }
            )
        for col in health["column_nulls"]:
            flat.append(
                {
                    "Section": "Column Nulls",
                    "Metric": col["field_name"],
                    "Value": f"{col['null_pct']:.1f}%",
                }
            )
        for col in health["column_sizes"]:
            flat.append(
                {
                    "Section": "Column Sizes",
                    "Metric": col["field_name"],
                    "Value": f"{format_bytes(col['total_bytes'])} ({col['pct_of_total']:.1f}%)",
                }
            )
        for col in health["column_bounds"]:
            flat.append(
                {
                    "Section": "Column Bounds",
                    "Metric": col["field_name"],
                    "Value": f"{col['min_value']} .. {col['max_value']}",
                }
            )
        emit(console, flat, ["Section", "Metric", "Value"], fmt, title="Table Health")
        return

    console.print()

    # --- File Health ---
    fh = health["file_health"]
    console.print("[bold]File Health[/bold]")
    if fh["file_count"] == 0:
        console.print("  [dim]No data files[/dim]")
    else:
        console.print(
            f"  {fh['file_count']} data files  |  "
            f"avg {format_bytes(fh['avg_size'])}  |  "
            f"min {format_bytes(fh['min_size'])}  |  "
            f"max {format_bytes(fh['max_size'])}  |  "
            f"median {format_bytes(fh['median_size'])}"
        )
        if fh["small_file_warning"]:
            console.print(
                f"  [yellow]Warning: {fh['small_file_count']} small files "
                f"(< 32 MB) -- consider compaction[/yellow]"
            )
        else:
            console.print("  [green]No small files detected[/green]")

    # --- Delete Files ---
    console.print()
    df = health["delete_files"]
    console.print("[bold]Delete Files[/bold]")
    console.print(
        f"  {df['data_manifest_count']} data manifests  |  "
        f"{df['delete_manifest_count']} delete manifests"
    )
    if df["compaction_recommended"]:
        console.print(
            f"  [yellow]Compaction recommended -- "
            f"{df['delete_manifest_count']} delete manifest"
            f"{'s' if df['delete_manifest_count'] != 1 else ''} pending[/yellow]"
        )
    else:
        console.print("  [green]No compaction needed[/green]")

    # --- Partitions ---
    console.print()
    parts = health["partitions"]
    console.print(f"[bold]Partitions[/bold]  [dim]({parts['count']} total)[/dim]")
    if parts["rows"]:
        pt = RichTable(show_header=True, box=None, padding=(0, 2))
        pt.add_column("Partition", style="cyan")
        pt.add_column("Files", justify="right")
        pt.add_column("Rows", justify="right", style="green")
        pt.add_column("Size", justify="right")
        pt.add_column("Skew", justify="center")
        for p in parts["rows"]:
            skew_tag = "[yellow]skewed[/yellow]" if p.get("skewed") else ""
            pt.add_row(
                p["partition"],
                str(p["file_count"]),
                f"{p['record_count']:,}",
                format_bytes(p["total_size"]),
                skew_tag,
            )
        console.print(pt)
        if parts["skewed_partitions"]:
            console.print(
                f"  [yellow]Skew detected in {len(parts['skewed_partitions'])} "
                f"partition{'s' if len(parts['skewed_partitions']) != 1 else ''} "
                f"(ratio: {parts['skew_ratio']:.1f}x avg)[/yellow]"
            )
    else:
        console.print("  [dim]Unpartitioned[/dim]")

    overlap = health["partition_overlap"]
    if overlap["overlap_warning"]:
        console.print(
            f"\n  [yellow]Overlap: {overlap['overlapping_pairs']} file pair"
            f"{'s' if overlap['overlapping_pairs'] != 1 else ''} "
            f"with overlapping bounds -- may hurt scan performance[/yellow]"
        )

    # --- Column Null Rates ---
    console.print()
    console.print("[bold]Column Null Rates[/bold]")
    if health["column_nulls"]:
        for col in health["column_nulls"]:
            pct = col["null_pct"]
            if pct >= 50:
                tag = f"[red]{pct:.1f}% null[/red]"
            elif pct >= 10:
                tag = f"[yellow]{pct:.1f}% null[/yellow]"
            elif pct > 0:
                tag = f"[dim]{pct:.1f}% null[/dim]"
            else:
                tag = "[green]0%[/green]"
            console.print(f"  {col['field_name']:<25} {tag}")
    else:
        console.print("  [dim]No column stats available[/dim]")

    # --- Column Sizes ---
    console.print()
    console.print("[bold]Column Sizes[/bold]  [dim](% of total storage)[/dim]")
    if health["column_sizes"]:
        max_pct = max(c["pct_of_total"] for c in health["column_sizes"])
        bar_scale = 20 / max_pct if max_pct > 0 else 1
        for col in health["column_sizes"]:
            bar_len = int(col["pct_of_total"] * bar_scale)
            bar = "\u2588" * bar_len
            console.print(
                f"  {col['field_name']:<25} {col['pct_of_total']:>5.1f}%  "
                f"{format_bytes(col['total_bytes']):>10}  [cyan]{bar}[/cyan]"
            )
    else:
        console.print("  [dim]No column size data available[/dim]")

    # --- Column Bounds ---
    console.print()
    console.print("[bold]Column Bounds[/bold]")
    if health["column_bounds"]:
        for col in health["column_bounds"]:
            console.print(f"  {col['field_name']:<25} {col['min_value']}  ..  {col['max_value']}")
    else:
        console.print("  [dim]No bounds data available[/dim]")
    console.print()


# ─── snapshot-detail ──────────────────────────────────────────


def render_snapshot_detail(
    console: Console, tbl: IcebergTable, snapshot_id: int, fmt: OutputFormat = OutputFormat.TABLE
) -> None:
    """Render a deep-dive into a single snapshot."""
    snap = _find_snapshot(tbl, snapshot_id)
    if not snap:
        console.print(f"[red]Snapshot {snapshot_id} not found[/red]")
        return

    if fmt != OutputFormat.TABLE:
        detail: dict[str, Any] = {
            "Snapshot ID": str(snap.snapshot_id),
            "Parent": str(snap.parent_snapshot_id or "None"),
            "Timestamp": format_timestamp_ms(snap.timestamp_ms),
            "Operation": snap.summary.operation.value if snap.summary else "N/A",
            "Manifest List": truncate_path(snap.manifest_list),
            "Schema ID": str(snap.schema_id or "N/A"),
        }
        if snap.summary:
            for key, value in snap.summary.additional_properties.items():
                detail[key] = str(value)
        emit(
            console,
            [detail],
            list(detail.keys()),
            fmt,
            title=f"Snapshot {snapshot_id}",
        )
        return

    info = RichTable(show_header=False, box=None, padding=(0, 2))
    info.add_column("Key", style="bold cyan")
    info.add_column("Value")
    info.add_row("Snapshot ID", str(snap.snapshot_id))
    info.add_row("Parent", str(snap.parent_snapshot_id or "None"))
    info.add_row("Timestamp", format_timestamp_ms(snap.timestamp_ms))
    info.add_row("Operation", snap.summary.operation.value if snap.summary else "N/A")
    info.add_row("Manifest List", truncate_path(snap.manifest_list))
    info.add_row("Schema ID", str(snap.schema_id or "N/A"))

    if snap.summary:
        for key, value in snap.summary.additional_properties.items():
            info.add_row(f"  {key}", str(value))

    console.print(Panel(info, title=f"[bold]Snapshot {snapshot_id}[/bold]", border_style="blue"))

    render_manifests(console, tbl, snapshot_id=snapshot_id, fmt=fmt)
    console.print()
    render_files(console, tbl, snapshot_id=snapshot_id, fmt=fmt)


# ─── summary (dashboard) ─────────────────────────────────────


def collect_summary(tbl: IcebergTable) -> dict[str, Any]:
    """Collect high-level table metrics for the summary dashboard."""
    md = tbl.metadata
    files_data = collect_files(tbl)
    total_size = sum(r.get("file_size_raw", 0) for r in files_data)
    total_rows = sum(r.get("record_count_raw", 0) for r in files_data)

    try:
        partition_count = tbl.inspect.partitions().num_rows
    except Exception:
        partition_count = 0

    recent_ops: list[dict[str, str]] = []
    for snap in reversed(md.snapshots[-5:]):
        op = snap.summary.operation.value if snap.summary else "unknown"
        props = snap.summary.additional_properties if snap.summary else {}
        added = props.get("added-records", "0")
        deleted = props.get("deleted-records", "0")
        recent_ops.append(
            {
                "operation": op,
                "added_records": added,
                "deleted_records": deleted,
                "timestamp": format_timestamp_ms(snap.timestamp_ms),
            }
        )

    return {
        "table_name": tbl.name(),
        "format_version": md.format_version,
        "location": md.location,
        "snapshot_count": len(md.snapshots),
        "schema_field_count": len(tbl.schema().fields),
        "data_file_count": len(files_data),
        "partition_count": partition_count,
        "total_size": format_bytes(total_size),
        "total_rows": f"{total_rows:,}",
        "last_updated": format_timestamp_ms(md.last_updated_ms),
        "recent_operations": recent_ops,
    }


def render_summary(
    console: Console, tbl: IcebergTable, fmt: OutputFormat = OutputFormat.TABLE
) -> None:
    """Render a single-screen summary dashboard."""
    data = collect_summary(tbl)

    if fmt != OutputFormat.TABLE:
        emit(console, [data], list(data.keys()), fmt, title="Summary")
        return

    console.print()
    console.print(
        f"[bold]Table:[/bold] {data['table_name']} [dim](v{data['format_version']})[/dim]"
    )
    console.print(f"[bold]Location:[/bold] {data['location']}")
    console.print()

    grid = RichTable(show_header=False, box=None, padding=(0, 3))
    grid.add_column("Label1", style="bold cyan")
    grid.add_column("Value1")
    grid.add_column("Label2", style="bold cyan")
    grid.add_column("Value2")

    grid.add_row(
        "Snapshots",
        str(data["snapshot_count"]),
        "Schema Fields",
        str(data["schema_field_count"]),
    )
    grid.add_row(
        "Data Files",
        str(data["data_file_count"]),
        "Partitions",
        str(data["partition_count"]),
    )
    grid.add_row(
        "Total Size",
        data["total_size"],
        "Total Rows",
        data["total_rows"],
    )
    grid.add_row("Last Updated", data["last_updated"], "", "")

    console.print(Panel(grid, title="[bold]Summary[/bold]", border_style="blue"))

    if data["recent_operations"]:
        console.print("\n[bold]Recent Operations:[/bold]")
        for op in data["recent_operations"]:
            try:
                added = int(op.get("added_records", 0))
            except (ValueError, TypeError):
                added = 0
            try:
                deleted = int(op.get("deleted_records", 0))
            except (ValueError, TypeError):
                deleted = 0
            parts = [f"[yellow]{op['operation']}[/yellow]"]
            if added:
                parts.append(f"[green]+{added:,} rows[/green]")
            if deleted:
                parts.append(f"[red]-{deleted:,} rows[/red]")
            parts.append(f"[dim]{op['timestamp']}[/dim]")
            console.print(f"  {'  '.join(parts)}")
    console.print()


# ─── diff ─────────────────────────────────────────────────────


def collect_diff(tbl: IcebergTable, snap_id_1: int, snap_id_2: int) -> dict[str, Any]:
    """Compute the difference between two snapshots."""
    for sid in (snap_id_1, snap_id_2):
        if _find_snapshot(tbl, sid) is None:
            raise ValueError(f"Snapshot {sid} not found in table {tbl.name()}")

    files_1 = {
        row.get("file_path", ""): row
        for row in tbl.inspect.files(snapshot_id=snap_id_1).to_pylist()
    }
    files_2 = {
        row.get("file_path", ""): row
        for row in tbl.inspect.files(snapshot_id=snap_id_2).to_pylist()
    }

    paths_1 = set(files_1.keys())
    paths_2 = set(files_2.keys())
    added_paths = paths_2 - paths_1
    deleted_paths = paths_1 - paths_2

    added_files = [
        {
            "file_path": truncate_path(p),
            "record_count": files_2[p].get("record_count", 0),
            "file_size": files_2[p].get("file_size_in_bytes", 0),
        }
        for p in sorted(added_paths)
    ]
    deleted_files = [
        {
            "file_path": truncate_path(p),
            "record_count": files_1[p].get("record_count", 0),
            "file_size": files_1[p].get("file_size_in_bytes", 0),
        }
        for p in sorted(deleted_paths)
    ]

    snap_2 = _find_snapshot(tbl, snap_id_2)
    operation = snap_2.summary.operation.value if snap_2 and snap_2.summary else "unknown"

    total_added_size = sum(f["file_size"] for f in added_files)
    total_added_rows = sum(f["record_count"] for f in added_files)
    total_deleted_size = sum(f["file_size"] for f in deleted_files)
    total_deleted_rows = sum(f["record_count"] for f in deleted_files)

    return {
        "snap_id_1": snap_id_1,
        "snap_id_2": snap_id_2,
        "operation": operation,
        "added_count": len(added_files),
        "deleted_count": len(deleted_files),
        "added_size": total_added_size,
        "added_rows": total_added_rows,
        "deleted_size": total_deleted_size,
        "deleted_rows": total_deleted_rows,
        "net_files": len(added_files) - len(deleted_files),
        "net_size": total_added_size - total_deleted_size,
        "net_rows": total_added_rows - total_deleted_rows,
        "added_files": added_files,
        "deleted_files": deleted_files,
    }


def render_diff(
    console: Console,
    tbl: IcebergTable,
    snap_id_1: int,
    snap_id_2: int,
    fmt: OutputFormat = OutputFormat.TABLE,
) -> None:
    """Render the difference between two snapshots."""
    data = collect_diff(tbl, snap_id_1, snap_id_2)

    if fmt != OutputFormat.TABLE:
        emit(console, [data], list(data.keys()), fmt, title="Snapshot Diff")
        return

    console.print()
    console.print(
        f"[bold]Snapshot {data['snap_id_1']} -> {data['snap_id_2']}[/bold] "
        f"[yellow]({data['operation']})[/yellow]"
    )

    console.print(
        f"  [green]+ {data['added_count']} files added[/green] "
        f"({format_bytes(data['added_size'])}, {data['added_rows']:,} rows)"
    )
    console.print(
        f"  [red]- {data['deleted_count']} files deleted[/red] "
        f"({format_bytes(data['deleted_size'])}, {data['deleted_rows']:,} rows)"
    )

    sign = "+" if data["net_size"] >= 0 else ""
    console.print(
        f"  [bold]Net:[/bold] {sign}{data['net_files']} files, "
        f"{sign}{format_bytes(data['net_size'])}, "
        f"{sign}{data['net_rows']:,} rows"
    )

    if data["added_files"]:
        console.print("\n  [bold green]Added:[/bold green]")
        for f in data["added_files"]:
            console.print(
                f"    {f['file_path']}  {f['record_count']:,} rows  {format_bytes(f['file_size'])}"
            )

    if data["deleted_files"]:
        console.print("\n  [bold red]Deleted:[/bold red]")
        for f in data["deleted_files"]:
            console.print(
                f"    {f['file_path']}  {f['record_count']:,} rows  {format_bytes(f['file_size'])}"
            )
    console.print()


# ─── list-tables ──────────────────────────────────────────────


def render_list_tables(
    console: Console,
    tables_by_ns: dict[str, list[str]],
    fmt: OutputFormat = OutputFormat.TABLE,
) -> None:
    """Render the namespace/table listing."""
    if fmt != OutputFormat.TABLE:
        flat: list[dict[str, Any]] = []
        for ns, tables in tables_by_ns.items():
            for t in tables:
                flat.append({"Namespace": ns, "Table": t})
        emit(console, flat, ["Namespace", "Table"], fmt, title="Tables")
        return

    for ns, tables in tables_by_ns.items():
        console.print(f"\n[bold]Namespace:[/bold] [cyan]{ns}[/cyan]")
        if tables:
            for t in tables:
                console.print(f"  {t}")
        else:
            console.print("  [dim](no tables)[/dim]")
    console.print()


# ─── tree (signature feature) ────────────────────────────────


def render_metadata_tree(
    console: Console,
    tbl: IcebergTable,
    snapshot_id: int | None = None,
    max_files: int = 10,
    all_snapshots: bool = False,
) -> None:
    """Render the full metadata hierarchy as a Rich Tree.

    Table
    +-- Snapshot <id> [operation] @ timestamp
        +-- Manifest List: snap-<id>.avro
            +-- Manifest: <path> (N data files, 45% of rows)
            |   +-- file1.parquet (1,234 rows, 5.6 MB)
            |   +-- file2.parquet (5,678 rows, 12.3 MB)
            +-- Manifest: <path> (M data files)
                +-- ...
    """
    root = Tree(
        f"[bold blue]Table:[/bold blue] [white]{tbl.name()}[/white]  "
        f"[dim](v{tbl.metadata.format_version})[/dim]"
    )

    if all_snapshots:
        snaps = tbl.metadata.snapshots
    elif snapshot_id:
        snap = _find_snapshot(tbl, snapshot_id)
        if not snap:
            console.print(f"[red]Snapshot {snapshot_id} not found[/red]")
            return
        snaps = [snap]
    else:
        if tbl.metadata.current_snapshot_id is None:
            console.print("[yellow]Table has no snapshots[/yellow]")
            return
        snap = _find_snapshot(tbl, tbl.metadata.current_snapshot_id)
        snaps = [snap]

    for snap in snaps:
        _add_snapshot_branch(root, tbl, snap, max_files)

    console.print()
    console.print(root)
    console.print()


def _add_snapshot_branch(root: Tree, tbl: IcebergTable, snap, max_files: int) -> None:
    """Add a single snapshot branch to the metadata tree."""
    op = snap.summary.operation.value if snap.summary else "unknown"
    current_marker = ""
    if snap.snapshot_id == tbl.metadata.current_snapshot_id:
        current_marker = " [bold green](current)[/bold green]"

    snap_branch = root.add(
        f"[bold yellow]Snapshot[/bold yellow] [white]{snap.snapshot_id}[/white] "
        f"[dim][{op}][/dim] @ {format_timestamp_ms(snap.timestamp_ms)}{current_marker}"
    )

    ml_branch = snap_branch.add(
        f"[bold cyan]Manifest List:[/bold cyan] [dim]{truncate_path(snap.manifest_list)}[/dim]"
    )

    try:
        manifest_files = snap.manifests(tbl.io)
    except Exception as exc:
        ml_branch.add(f"[red]Failed to read manifests: {exc}[/red]")
        return

    all_entries_by_manifest: list[list] = []
    total_rows = 0
    all_sizes: list[int] = []
    for manifest in manifest_files:
        try:
            entries = manifest.fetch_manifest_entry(tbl.io)
        except Exception:
            log.debug(
                "Failed to fetch entries for manifest %s",
                manifest.manifest_path,
                exc_info=True,
            )
            entries = []
        all_entries_by_manifest.append(entries)
        for entry in entries:
            total_rows += entry.data_file.record_count
            all_sizes.append(entry.data_file.file_size_in_bytes)

    avg_size = statistics.mean(all_sizes) if all_sizes else 0

    for manifest, entries in zip(manifest_files, all_entries_by_manifest, strict=False):
        added = manifest.added_files_count or 0
        existing = manifest.existing_files_count or 0
        total_files = added + existing
        manifest_rows = sum(e.data_file.record_count for e in entries)
        pct = f", {manifest_rows / total_rows:.0%} of rows" if total_rows else ""

        m_branch = ml_branch.add(
            f"[bold green]Manifest:[/bold green] "
            f"[dim]{truncate_path(manifest.manifest_path)}[/dim] "
            f"({total_files} files, {format_bytes(manifest.manifest_length)}{pct})"
        )

        for shown, entry in enumerate(entries):
            if shown >= max_files:
                remaining = len(entries) - shown
                if remaining > 0:
                    m_branch.add(f"[dim]... and {remaining} more files[/dim]")
                break
            df = entry.data_file
            color = _size_color(df.file_size_in_bytes, avg_size)
            m_branch.add(
                f"[{color}]{truncate_path(df.file_path)}[/{color}] "
                f"({df.record_count:,} rows, {format_bytes(df.file_size_in_bytes)})"
            )


# ─── catalog overview (metadata-only, no inspect calls) ──────


def _table_meta_summary(tbl: IcebergTable) -> dict[str, Any]:
    """Extract lightweight metadata for a single table — no S3 manifest reads.

    Pulls storage stats from the current snapshot's summary dict which is
    already embedded in the metadata JSON (zero extra IO).
    """
    md = tbl.metadata
    snapshot_count = len(md.snapshots)
    last_updated = md.last_updated_ms

    partition_desc = "unpartitioned"
    schema = None
    try:
        schema = tbl.schema()
        field_by_id = {f.field_id: f for f in schema.fields}
        if md.default_spec_id < len(md.partition_specs):
            spec = md.partition_specs[md.default_spec_id]
            if spec.fields:
                parts = []
                for pf in spec.fields:
                    src = field_by_id.get(pf.source_id)
                    src_name = src.name if src else f"id={pf.source_id}"
                    parts.append(f"{pf.transform}({src_name})")
                partition_desc = ", ".join(parts)
    except Exception:
        pass

    schema_count = len(tbl.schemas()) if hasattr(tbl, "schemas") else 1

    try:
        raw_name = tbl.name()
        tbl_name = ".".join(raw_name) if isinstance(raw_name, tuple) else str(raw_name)
    except Exception:
        tbl_name = str(tbl)

    total_files = 0
    total_records = 0
    total_size = 0
    try:
        current_snap = md.snapshots[-1] if md.snapshots else None
        if current_snap and current_snap.summary:
            props = current_snap.summary.additional_properties
            total_files = int(props.get("total-data-files", 0))
            total_records = int(props.get("total-records", 0))
            total_size = int(props.get("total-files-size", 0))
    except Exception:
        pass

    fields: dict[str, str] = {}
    if schema:
        for f in schema.fields:
            fields[f.name] = str(f.field_type)

    return {
        "name": tbl_name,
        "format_version": md.format_version,
        "snapshot_count": snapshot_count,
        "schema_count": schema_count,
        "current_schema_id": md.current_schema_id,
        "last_updated_ms": last_updated,
        "last_updated": format_timestamp_ms(last_updated),
        "partition": partition_desc,
        "total_files": total_files,
        "total_records": total_records,
        "total_size": total_size,
        "fields": fields,
    }


def _find_schema_conflicts(tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Find fields that appear in multiple tables with different types."""
    field_types: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for t in tables:
        short = t["name"].split(".")[-1] if "." in t["name"] else t["name"]
        for fname, ftype in t.get("fields", {}).items():
            field_types[fname][ftype].append(short)

    conflicts: list[dict[str, Any]] = []
    for fname, type_map in sorted(field_types.items()):
        if len(type_map) > 1:
            details = []
            for ftype, tbls in sorted(type_map.items()):
                details.append({"type": ftype, "tables": tbls})
            conflicts.append({"field": fname, "types": details})
    return conflicts


def collect_namespace_overview(
    config: CatalogConfig, namespace: str, table_ids: list[str]
) -> dict[str, Any]:
    """Collect metadata-only overview for all tables in a namespace."""
    from iceberg_meta.catalog import get_table

    tables: list[dict[str, Any]] = []
    errors: list[str] = []

    for tid in table_ids:
        try:
            tbl = get_table(config, tid)
            tables.append(_table_meta_summary(tbl))
        except Exception as exc:
            errors.append(f"{tid}: {exc}")

    tables.sort(key=lambda t: t["last_updated_ms"] or 0)

    total_size = sum(t["total_size"] for t in tables)
    total_files = sum(t["total_files"] for t in tables)
    total_records = sum(t["total_records"] for t in tables)
    schema_conflicts = _find_schema_conflicts(tables)

    return {
        "namespace": namespace,
        "table_count": len(tables),
        "total_size": total_size,
        "total_files": total_files,
        "total_records": total_records,
        "tables": tables,
        "schema_conflicts": schema_conflicts,
        "errors": errors,
    }


def collect_warehouse_overview(
    config: CatalogConfig, tables_by_ns: dict[str, list[str]]
) -> dict[str, Any]:
    """Collect metadata-only overview across all namespaces."""
    from iceberg_meta.catalog import get_table

    all_tables: list[dict[str, Any]] = []
    ns_summaries: list[dict[str, Any]] = []
    version_counts: dict[int, int] = defaultdict(int)
    errors: list[str] = []

    for ns, table_ids in sorted(tables_by_ns.items()):
        ns_tables: list[dict[str, Any]] = []
        for tid in table_ids:
            try:
                tbl = get_table(config, tid)
                meta = _table_meta_summary(tbl)
                meta["namespace"] = ns
                ns_tables.append(meta)
                all_tables.append(meta)
                version_counts[meta["format_version"]] += 1
            except Exception as exc:
                errors.append(f"{tid}: {exc}")
        ns_size = sum(t["total_size"] for t in ns_tables)
        ns_files = sum(t["total_files"] for t in ns_tables)
        ns_records = sum(t["total_records"] for t in ns_tables)
        ns_summaries.append(
            {
                "namespace": ns,
                "table_count": len(ns_tables),
                "total_size": ns_size,
                "total_files": ns_files,
                "total_records": ns_records,
            }
        )

    all_tables.sort(key=lambda t: t["last_updated_ms"] or 0)

    total_size = sum(t["total_size"] for t in all_tables)
    total_files = sum(t["total_files"] for t in all_tables)
    total_records = sum(t["total_records"] for t in all_tables)

    stale_tables = all_tables[:5] if all_tables else []
    snapshot_hogs = sorted(all_tables, key=lambda t: t["snapshot_count"], reverse=True)[:5]

    return {
        "namespace_count": len(tables_by_ns),
        "total_tables": len(all_tables),
        "total_size": total_size,
        "total_files": total_files,
        "total_records": total_records,
        "namespaces": ns_summaries,
        "format_versions": dict(version_counts),
        "stalest_tables": stale_tables,
        "most_snapshots": snapshot_hogs,
        "errors": errors,
    }
