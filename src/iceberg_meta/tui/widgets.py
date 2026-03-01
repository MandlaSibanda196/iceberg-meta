"""Custom widgets for the iceberg-meta TUI."""

from __future__ import annotations

import asyncio
from functools import partial
from typing import Any

from textual.message import Message
from textual.widgets import DataTable, Static, Tree

from iceberg_meta.catalog import CatalogConfig, get_table, list_all_tables
from iceberg_meta.formatters import (
    collect_files,
    collect_manifests,
    collect_namespace_overview,
    collect_snapshots,
    collect_summary,
    collect_table_health,
    collect_table_info,
    collect_warehouse_overview,
)
from iceberg_meta.utils import format_bytes, format_timestamp_ms, truncate_path

# ---------------------------------------------------------------------------
# Sidebar: catalog browser
# ---------------------------------------------------------------------------


class TableBrowser(Tree[str]):
    """Tree widget showing namespace > table hierarchy."""

    class TableSelected(Message):
        """Posted when the user selects a table."""

        def __init__(self, table_id: str) -> None:
            super().__init__()
            self.table_id = table_id

    class NamespaceSelected(Message):
        """Posted when the user selects a namespace node."""

        def __init__(self, ns_name: str, table_ids: list[str]) -> None:
            super().__init__()
            self.ns_name = ns_name
            self.table_ids = table_ids

    class CatalogRootSelected(Message):
        """Posted when the user selects the catalog root node."""

        def __init__(self, tables_by_ns: dict[str, list[str]]) -> None:
            super().__init__()
            self.tables_by_ns = tables_by_ns

    def __init__(self, config: CatalogConfig, **kwargs: Any) -> None:
        super().__init__("Catalog", **kwargs)
        self.config = config
        self.show_root = True
        self._tables_by_ns: dict[str, list[str]] = {}

    def on_mount(self) -> None:
        self.loading = True
        self.run_worker(self._load_tables(), exclusive=True)

    async def _load_tables(self) -> None:
        try:
            tables_by_ns = await asyncio.to_thread(list_all_tables, self.config)
            self._tables_by_ns = tables_by_ns
            self.clear()
            for ns, tables in sorted(tables_by_ns.items()):
                ns_node = self.root.add(ns, expand=True)
                for tbl in sorted(tables):
                    short_name = tbl.split(".")[-1] if "." in tbl else tbl
                    ns_node.add_leaf(short_name, data=tbl)
            self.root.expand()
        except Exception as exc:
            self.clear()
            self.root.add_leaf(f"Failed to load catalog: {exc}")
            self.root.expand()
            self.app.notify(f"Catalog error: {exc}", severity="error")
        finally:
            self.loading = False

    def on_tree_node_selected(self, event: Tree.NodeSelected[str]) -> None:
        node = event.node
        if node.is_root:
            self.post_message(self.CatalogRootSelected(dict(self._tables_by_ns)))
        elif node.children:
            ns_label = str(node.label)
            table_ids = [child.data for child in node.children if child.data]
            self.post_message(self.NamespaceSelected(ns_label, table_ids))
        elif node.data:
            self.post_message(self.TableSelected(node.data))

    async def action_reload(self) -> None:
        self.loading = True
        self.run_worker(self._load_tables(), exclusive=True)


# ---------------------------------------------------------------------------
# Summary panel
# ---------------------------------------------------------------------------


class SummaryPanel(Static):
    """Displays the summary dashboard for a table."""

    def on_mount(self) -> None:
        self.border_title = "Summary"

    def update_table(self, config: CatalogConfig, table_id: str) -> None:
        self.loading = True
        self.run_worker(self._load(config, table_id), exclusive=True)

    async def _load(self, config: CatalogConfig, table_id: str) -> None:
        try:
            tbl = await asyncio.to_thread(get_table, config, table_id)
            data = await asyncio.to_thread(collect_summary, tbl)
            info_rows = await asyncio.to_thread(collect_table_info, tbl)

            lines: list[str] = []
            lines.append(f"[bold]{data['table_name']}[/bold]  [dim]v{data['format_version']}[/dim]")
            lines.append(f"[dim]{data['location']}[/dim]\n")

            lines.append(
                f"  [cyan]Snapshots:[/cyan]  {data['snapshot_count']}      "
                f"[cyan]Schema Fields:[/cyan]  {data['schema_field_count']}"
            )
            lines.append(
                f"  [cyan]Data Files:[/cyan] {data['data_file_count']}      "
                f"[cyan]Partitions:[/cyan]    {data['partition_count']}"
            )
            lines.append(
                f"  [cyan]Total Size:[/cyan] {data['total_size']}    "
                f"[cyan]Total Rows:[/cyan]    {data['total_rows']}"
            )
            lines.append(f"  [cyan]Last Updated:[/cyan] {data['last_updated']}")

            try:
                health = await asyncio.to_thread(collect_table_health, tbl)
                fh = health["file_health"]
                if fh["file_count"] > 0:
                    health_str = f"avg {format_bytes(fh['avg_size'])} ({fh['file_count']} files"
                    if fh["small_file_warning"]:
                        health_str += f", [yellow]{fh['small_file_count']} small[/yellow]"
                    health_str += ")"
                    lines.append(f"  [cyan]File Health:[/cyan] {health_str}")

                df = health["delete_files"]
                if df["compaction_recommended"]:
                    lines.append(
                        f"  [cyan]Delete Files:[/cyan] "
                        f"[yellow]{df['delete_manifest_count']} delete manifest"
                        f"{'s' if df['delete_manifest_count'] != 1 else ''} "
                        f"(compaction recommended)[/yellow]"
                    )
                else:
                    lines.append("  [cyan]Delete Files:[/cyan] none")
            except Exception:
                pass

            if data["recent_operations"]:
                lines.append("\n[bold]Recent Operations[/bold]")
                for op in data["recent_operations"]:
                    try:
                        added = int(op.get("added_records", 0))
                    except (ValueError, TypeError):
                        added = 0
                    try:
                        deleted = int(op.get("deleted_records", 0))
                    except (ValueError, TypeError):
                        deleted = 0
                    parts = [f"  [yellow]{op['operation']}[/yellow]"]
                    if added:
                        parts.append(f"[green]+{added:,}[/green]")
                    if deleted:
                        parts.append(f"[red]-{deleted:,}[/red]")
                    parts.append(f"[dim]{op['timestamp']}[/dim]")
                    lines.append("  ".join(parts))

            for row in info_rows:
                if row["Key"] == "Table UUID":
                    lines.append(f"\n  [cyan]UUID:[/cyan] {row['Value']}")
                    break

            self.update("\n".join(lines))
        except Exception as exc:
            self.update(f"[red]Failed to load summary: {exc}[/red]")
            self.app.notify(f"Summary error: {exc}", severity="error")
        finally:
            self.loading = False


# ---------------------------------------------------------------------------
# Snapshots table
# ---------------------------------------------------------------------------


class SnapshotsPanel(Static):
    """DataTable of snapshots."""

    class SnapshotHighlighted(Message):
        def __init__(self, snapshot_id: str) -> None:
            super().__init__()
            self.snapshot_id = snapshot_id

    def compose(self):
        yield DataTable(id="snapshots-dt")

    def on_mount(self) -> None:
        self.border_title = "Snapshots"

    def update_table(self, config: CatalogConfig, table_id: str) -> None:
        self.loading = True
        self.run_worker(self._load(config, table_id), exclusive=True)

    async def _load(self, config: CatalogConfig, table_id: str) -> None:
        try:
            tbl = await asyncio.to_thread(get_table, config, table_id)
            data = await asyncio.to_thread(collect_snapshots, tbl)

            dt = self.query_one("#snapshots-dt", DataTable)
            dt.clear(columns=True)
            dt.add_columns("Snapshot ID", "Timestamp", "Parent ID", "Operation", "Summary")
            for row in data:
                dt.add_row(
                    row["Snapshot ID"],
                    row["Timestamp"],
                    row["Parent ID"],
                    row["Operation"],
                    row.get("Summary", "")[:60],
                )
            dt.cursor_type = "row"
            self.border_subtitle = f"{len(data)} snapshots"
        except Exception as exc:
            dt = self.query_one("#snapshots-dt", DataTable)
            dt.clear(columns=True)
            dt.add_column("Error")
            dt.add_row(f"Failed to load snapshots: {exc}")
            self.app.notify(f"Snapshots error: {exc}", severity="error")
        finally:
            self.loading = False


# ---------------------------------------------------------------------------
# Schema tree
# ---------------------------------------------------------------------------


class SchemaPanel(Static):
    """Tree view of table schema with full evolution history and diffs."""

    def compose(self):
        yield Tree("Schema", id="schema-tree")

    def on_mount(self) -> None:
        self.border_title = "Schema"

    def update_table(self, config: CatalogConfig, table_id: str) -> None:
        self.loading = True
        self.run_worker(self._load(config, table_id), exclusive=True)

    async def _load(self, config: CatalogConfig, table_id: str) -> None:
        try:
            tbl = await asyncio.to_thread(get_table, config, table_id)
            tree = self.query_one("#schema-tree", Tree)
            tree.clear()

            schemas = tbl.schemas()
            current_id = tbl.metadata.current_schema_id
            sorted_ids = sorted(schemas.keys())

            tree.root.set_label(
                f"[bold]Schema Evolution[/bold]  "
                f"[dim]{len(sorted_ids)} version{'s' if len(sorted_ids) != 1 else ''}  "
                f"| current: v{current_id}[/dim]"
            )

            prev_schema = None
            for schema_id in sorted_ids:
                schema = schemas[schema_id]
                is_current = schema_id == current_id
                suffix = "  [bold green](current)[/bold green]" if is_current else ""
                node = tree.root.add(
                    f"[bold]Schema v{schema_id}[/bold]  "
                    f"[dim]{len(schema.fields)} fields[/dim]{suffix}"
                )

                if prev_schema is not None:
                    self._add_diff(node, prev_schema, schema)

                for field in schema.fields:
                    self._add_field(node, field)

                if is_current:
                    node.expand()
                prev_schema = schema

            tree.root.expand()
        except Exception as exc:
            tree = self.query_one("#schema-tree", Tree)
            tree.clear()
            tree.root.set_label("Schema")
            tree.root.add_leaf(f"Failed to load schema: {exc}")
            tree.root.expand()
            self.app.notify(f"Schema error: {exc}", severity="error")
        finally:
            self.loading = False

    def _add_diff(self, parent, old_schema, new_schema) -> None:
        old_fields = {f.field_id: f for f in old_schema.fields}
        new_fields = {f.field_id: f for f in new_schema.fields}

        added = [fid for fid in new_fields if fid not in old_fields]
        removed = [fid for fid in old_fields if fid not in new_fields]
        changed = []
        for fid in new_fields:
            if fid in old_fields:
                of, nf = old_fields[fid], new_fields[fid]
                types_differ = str(of.field_type) != str(nf.field_type)
                if types_differ or of.name != nf.name or of.required != nf.required:
                    changed.append(fid)

        if not added and not removed and not changed:
            parent.add_leaf("[dim]no changes from previous version[/dim]")
            return

        diff_node = parent.add(
            f"[bold yellow]Changes from v{old_schema.schema_id}[/bold yellow]  "
            f"[dim]+{len(added)} -{len(removed)} ~{len(changed)}[/dim]"
        )
        for fid in added:
            f = new_fields[fid]
            diff_node.add_leaf(f"[green]+ {f.name} : {f.field_type}[/green]")
        for fid in removed:
            f = old_fields[fid]
            diff_node.add_leaf(f"[red]- {f.name} : {f.field_type}[/red]")
        for fid in changed:
            of, nf = old_fields[fid], new_fields[fid]
            parts = []
            if of.name != nf.name:
                parts.append(f"renamed {of.name} -> {nf.name}")
            if str(of.field_type) != str(nf.field_type):
                parts.append(f"type {of.field_type} -> {nf.field_type}")
            if of.required != nf.required:
                old_req = "required" if of.required else "optional"
                new_req = "required" if nf.required else "optional"
                parts.append(f"{old_req} -> {new_req}")
            diff_node.add_leaf(f"[yellow]~ {nf.name}: {', '.join(parts)}[/yellow]")
        diff_node.expand()

    def _add_field(self, parent, field) -> None:
        req = "required" if field.required else "optional"
        label = (
            f"[cyan]{field.name}[/cyan] : [yellow]{field.field_type}[/yellow] "
            f"[dim]({req}, id={field.field_id})[/dim]"
        )
        if hasattr(field.field_type, "fields"):
            branch = parent.add(label)
            for sub in field.field_type.fields:
                self._add_field(branch, sub)
        else:
            parent.add_leaf(label)


# ---------------------------------------------------------------------------
# Files table
# ---------------------------------------------------------------------------


class FilesPanel(Static):
    """DataTable of data files with a size distribution header."""

    SMALL_FILE_THRESHOLD = 32 * 1024 * 1024

    def compose(self):
        yield Static("", id="files-header")
        yield DataTable(id="files-dt")

    def on_mount(self) -> None:
        self.border_title = "Files"

    def update_table(self, config: CatalogConfig, table_id: str) -> None:
        self.loading = True
        self.run_worker(self._load(config, table_id), exclusive=True)

    async def _load(self, config: CatalogConfig, table_id: str) -> None:
        try:
            tbl = await asyncio.to_thread(get_table, config, table_id)
            data = await asyncio.to_thread(collect_files, tbl)

            dt = self.query_one("#files-dt", DataTable)
            dt.clear(columns=True)
            dt.add_columns("File Path", "Format", "Record Count", "File Size")
            for row in data:
                dt.add_row(row["File Path"], row["Format"], row["Record Count"], row["File Size"])
            dt.cursor_type = "row"

            total = len(data)
            raw_sizes = [r.get("file_size_raw", 0) for r in data]
            total_size = sum(raw_sizes)
            total_rows = sum(r.get("record_count_raw", 0) for r in data)
            self.border_subtitle = (
                f"{total} files  |  {format_bytes(total_size)}  |  {total_rows:,} rows"
            )

            header = self.query_one("#files-header", Static)
            if raw_sizes:
                import statistics as _stats

                small_count = sum(1 for s in raw_sizes if s < self.SMALL_FILE_THRESHOLD)
                parts = [
                    f"min {format_bytes(min(raw_sizes))}",
                    f"avg {format_bytes(int(_stats.mean(raw_sizes)))}",
                    f"median {format_bytes(int(_stats.median(raw_sizes)))}",
                    f"max {format_bytes(max(raw_sizes))}",
                ]
                header_text = "[dim]" + "  |  ".join(parts) + "[/dim]"
                if small_count:
                    header_text += (
                        f"  [yellow]{small_count} small "
                        f"file{'s' if small_count != 1 else ''} (< 32 MB)[/yellow]"
                    )
                header.update(header_text)
            else:
                header.update("")
        except Exception as exc:
            dt = self.query_one("#files-dt", DataTable)
            dt.clear(columns=True)
            dt.add_column("Error")
            dt.add_row(f"Failed to load files: {exc}")
            self.app.notify(f"Files error: {exc}", severity="error")
        finally:
            self.loading = False


# ---------------------------------------------------------------------------
# Metadata tree (signature feature)
# ---------------------------------------------------------------------------


class MetadataTreePanel(Static):
    """Full metadata hierarchy: snapshot > manifest list > manifests > files."""

    def compose(self):
        yield Tree("Metadata", id="meta-tree")

    def on_mount(self) -> None:
        self.border_title = "Metadata Tree"

    def update_table(self, config: CatalogConfig, table_id: str) -> None:
        self.loading = True
        self.run_worker(self._load(config, table_id), exclusive=True)

    async def _load(self, config: CatalogConfig, table_id: str) -> None:
        try:
            self.tbl = await asyncio.to_thread(get_table, config, table_id)
            tbl = self.tbl
            tree = self.query_one("#meta-tree", Tree)
            tree.clear()
            tree.root.set_label(f"Table: {tbl.name()}  (v{tbl.metadata.format_version})")

            if tbl.metadata.current_snapshot_id is None:
                tree.root.add_leaf("(no snapshots)")
                tree.root.expand()
                return

            # Add snapshots in reverse order (newest first) for better UX?
            # Or chronological? usually chronological or reverse chronological.
            # Current implementation did chronological (metadata.snapshots order).
            for snap in reversed(tbl.metadata.snapshots):
                self._add_snapshot_node(tree.root, tbl, snap)

            tree.root.expand()
        except Exception as exc:
            tree = self.query_one("#meta-tree", Tree)
            tree.clear()
            tree.root.set_label("Metadata")
            tree.root.add_leaf(f"Failed to load metadata tree: {exc}")
            tree.root.expand()
            self.app.notify(f"Tree error: {exc}", severity="error")
        finally:
            self.loading = False

    def _add_snapshot_node(self, root, tbl, snap) -> None:
        op = snap.summary.operation.value if snap.summary else "unknown"
        is_current = snap.snapshot_id == tbl.metadata.current_snapshot_id
        current = " [bold green](current)[/bold green]" if is_current else ""
        label = (
            f"[bold]Snapshot {snap.snapshot_id}[/bold]  "
            f"[dim][{op}][/dim]  "
            f"[dim]{format_timestamp_ms(snap.timestamp_ms)}[/dim]{current}"
        )
        node = root.add(label, data=snap)
        node.add_leaf("Loading...")
        if is_current:
            node.expand()

    def on_tree_node_expanded(self, event: Tree.NodeExpanded) -> None:
        node = event.node
        if not node.data:
            return
        
        # Check if it's a lazy node (has single "Loading..." child)
        if len(node.children) == 1 and str(node.children[0].label) == "Loading...":
            node.remove_children()
            data = node.data
            # Snapshot has snapshot_id
            if hasattr(data, "snapshot_id"): 
                self.run_worker(self._load_manifests(node, data))
            # ManifestFile has manifest_path
            elif hasattr(data, "manifest_path"):
                self.run_worker(self._load_entries(node, data))

    async def _load_manifests(self, node: Tree.Node, snap) -> None:
        try:
            # Need to run IO in thread
            manifest_files = await asyncio.to_thread(snap.manifests, self.tbl.io)
            
            ml_node = node.add(
                f"[bold cyan]Manifest List:[/bold cyan] [dim]{truncate_path(snap.manifest_list)}[/dim]"
            )
            
            # Pre-calculate totals if possible? 
            # Reading manifest files is needed to get totals.
            # But we want to LAZY load manifest files (entries) too.
            # So we iterate manifest_files (which are ManifestFile objects from the list)
            # and add nodes for them.
            
            for manifest in manifest_files:
                added = manifest.added_files_count or 0
                existing = manifest.existing_files_count or 0
                deleted = manifest.deleted_files_count or 0
                total = added + existing
                
                label = (
                    f"[bold green]Manifest:[/bold green] "
                    f"[dim]{truncate_path(manifest.manifest_path)}[/dim]  "
                    f"({total} files, {format_bytes(manifest.manifest_length)})"
                )
                mnode = ml_node.add(label, data=manifest)
                mnode.add_leaf("Loading...")
                
            ml_node.expand()
        except Exception as exc:
            node.add_leaf(f"[red]Failed to load manifests: {exc}[/red]")

    async def _load_entries(self, node: Tree.Node, manifest) -> None:
        try:
            entries = await asyncio.to_thread(manifest.fetch_manifest_entry, self.tbl.io)
            
            # Show max 50 files to prevent UI lag if manifest is huge
            MAX_FILES = 50
            for i, entry in enumerate(entries):
                if i >= MAX_FILES:
                    remaining = len(entries) - i
                    node.add_leaf(f"[dim]... and {remaining} more files[/dim]")
                    break
                    
                df = entry.data_file
                node.add_leaf(
                    f"[dim]{truncate_path(df.file_path)}[/dim]  "
                    f"({df.record_count:,} rows, {format_bytes(df.file_size_in_bytes)})"
                )
        except Exception as exc:
            node.add_leaf(f"[red]Failed to load entries: {exc}[/red]")



# ---------------------------------------------------------------------------
# Health panel
# ---------------------------------------------------------------------------


class HealthPanel(Static):
    """Comprehensive table health dashboard with Rich markup sections."""

    def on_mount(self) -> None:
        self.border_title = "Health"

    def update_table(self, config: CatalogConfig, table_id: str) -> None:
        self.loading = True
        self.run_worker(self._load(config, table_id), exclusive=True)

    async def _load(self, config: CatalogConfig, table_id: str) -> None:
        try:
            tbl = await asyncio.to_thread(get_table, config, table_id)
            health = await asyncio.to_thread(collect_table_health, tbl)
            self.update(self._format_health(health))
        except Exception as exc:
            self.update(f"[red]Failed to load health data: {exc}[/red]")
            self.app.notify(f"Health error: {exc}", severity="error")
        finally:
            self.loading = False

    def _format_health(self, health: dict) -> str:
        lines: list[str] = []

        # --- File Health ---
        fh = health["file_health"]
        lines.append("[bold]File Health[/bold]")
        if fh["file_count"] == 0:
            lines.append("  [dim]No data files[/dim]")
        else:
            lines.append(
                f"  {fh['file_count']} data files  |  "
                f"avg {format_bytes(fh['avg_size'])}  |  "
                f"min {format_bytes(fh['min_size'])}  |  "
                f"max {format_bytes(fh['max_size'])}  |  "
                f"median {format_bytes(fh['median_size'])}"
            )
            if fh["small_file_warning"]:
                lines.append(
                    f"  [yellow]{fh['small_file_count']} files below 32 MB — "
                    f"merging small files would improve read performance[/yellow]"
                )
            else:
                lines.append("  [green]File sizes look healthy[/green]")

        # --- Delete Files ---
        lines.append("")
        df = health["delete_files"]
        lines.append("[bold]Delete Files[/bold]")
        lines.append(
            f"  {df['data_manifest_count']} data manifests  |  "
            f"{df['delete_manifest_count']} delete manifests"
        )
        if df["compaction_recommended"]:
            lines.append(
                f"  [yellow]{df['delete_manifest_count']} pending delete"
                f"{'s' if df['delete_manifest_count'] != 1 else ''} — "
                f"rewriting data files would reclaim space[/yellow]"
            )
        else:
            lines.append("  [green]No pending deletes[/green]")

        # --- Partition Spec ---
        lines.append("")
        spec = health.get("partition_spec", [])
        lines.append("[bold]Partition Spec[/bold]")
        if spec:
            for ps in spec:
                lines.append(
                    f"  [cyan]{ps['name']}[/cyan] = "
                    f"[yellow]{ps['transform']}[/yellow]"
                    f"([dim]{ps['source_field']}[/dim])"
                )
        else:
            lines.append("  [dim]Unpartitioned[/dim]")

        # --- Partitions ---
        lines.append("")
        parts = health["partitions"]
        lines.append(f"[bold]Partition Values[/bold]  [dim]({parts['count']} total)[/dim]")
        if parts["rows"]:
            for p in parts["rows"]:
                skew_tag = "  [yellow]skewed[/yellow]" if p.get("skewed") else ""
                lines.append(
                    f"  {p['partition']:<30}  "
                    f"{p['file_count']:>3} files  "
                    f"{p['record_count']:>10,} rows  "
                    f"{format_bytes(p['total_size']):>10}{skew_tag}"
                )
            if parts["skewed_partitions"]:
                lines.append(
                    f"  [yellow]Skew detected in {len(parts['skewed_partitions'])} "
                    f"partition{'s' if len(parts['skewed_partitions']) != 1 else ''} "
                    f"(ratio: {parts['skew_ratio']:.1f}x avg)[/yellow]"
                )
        else:
            lines.append("  [dim]Unpartitioned[/dim]")

        # --- Partition Overlap ---
        overlap = health["partition_overlap"]
        if overlap["overlap_warning"] is None:
            lines.append("\n  [dim]Overlap detection skipped (too many data files)[/dim]")
        elif overlap["overlap_warning"]:
            lines.append(
                f"\n  [yellow]Overlap: {overlap['overlapping_pairs']} file pair"
                f"{'s' if overlap['overlapping_pairs'] != 1 else ''} "
                f"with overlapping bounds — may hurt scan performance[/yellow]"
            )

        # --- Column Null Rates ---
        lines.append("")
        lines.append("[bold]Column Null Rates[/bold]")
        if health["column_nulls"]:
            for col in health["column_nulls"]:
                pct = col["null_pct"]
                if pct >= 50:
                    tag = f"  [red]{pct:.1f}% null[/red]"
                elif pct >= 10:
                    tag = f"  [yellow]{pct:.1f}% null[/yellow]"
                elif pct > 0:
                    tag = f"  [dim]{pct:.1f}% null[/dim]"
                else:
                    tag = "  [green]0%[/green]"
                lines.append(f"  {col['field_name']:<25}{tag}")
        else:
            lines.append("  [dim]No column stats available[/dim]")

        # --- Column Sizes ---
        lines.append("")
        lines.append("[bold]Column Sizes[/bold]  [dim](% of total storage)[/dim]")
        if health["column_sizes"]:
            max_pct = max(c["pct_of_total"] for c in health["column_sizes"])
            bar_scale = 25 / max_pct if max_pct > 0 else 1
            for col in health["column_sizes"]:
                bar_len = max(1, int(col["pct_of_total"] * bar_scale))
                bar = "\u2501" * bar_len
                pct = col["pct_of_total"]
                if pct >= 30:
                    color = "red"
                elif pct >= 15:
                    color = "yellow"
                else:
                    color = "cyan"
                lines.append(
                    f"  {col['field_name']:<25} {pct:>5.1f}%  "
                    f"{format_bytes(col['total_bytes']):>10}  [{color}]{bar}[/{color}]"
                )
        else:
            lines.append("  [dim]No column size data available[/dim]")

        # --- Column Bounds ---
        lines.append("")
        lines.append("[bold]Column Bounds[/bold]")
        if health["column_bounds"]:
            for col in health["column_bounds"]:
                lines.append(
                    f"  {col['field_name']:<25} {col['min_value']}  ..  {col['max_value']}"
                )
        else:
            lines.append("  [dim]No bounds data available[/dim]")

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Manifests table
# ---------------------------------------------------------------------------


class ManifestsPanel(Static):
    """DataTable of manifest files."""

    def compose(self):
        yield DataTable(id="manifests-dt")

    def on_mount(self) -> None:
        self.border_title = "Manifests"

    def update_table(self, config: CatalogConfig, table_id: str) -> None:
        self.loading = True
        self.run_worker(self._load(config, table_id), exclusive=True)

    async def _load(self, config: CatalogConfig, table_id: str) -> None:
        try:
            tbl = await asyncio.to_thread(get_table, config, table_id)
            data = await asyncio.to_thread(collect_manifests, tbl)

            dt = self.query_one("#manifests-dt", DataTable)
            dt.clear(columns=True)
            dt.add_columns("Path", "Length", "Spec ID", "Content", "Added", "Existing", "Deleted")
            for row in data:
                dt.add_row(
                    row["Path"],
                    row["Length"],
                    row["Spec ID"],
                    row["Content"],
                    row["Added"],
                    row["Existing"],
                    row["Deleted"],
                )
            dt.cursor_type = "row"

            self.border_subtitle = f"{len(data)} manifests"
        except Exception as exc:
            dt = self.query_one("#manifests-dt", DataTable)
            dt.clear(columns=True)
            dt.add_column("Error")
            dt.add_row(f"Failed to load manifests: {exc}")
            self.app.notify(f"Manifests error: {exc}", severity="error")
        finally:
            self.loading = False


# ---------------------------------------------------------------------------
# Catalog / namespace overview
# ---------------------------------------------------------------------------

_STALE_DAYS_WARN = 7
_STALE_DAYS_CRIT = 30
_SNAPSHOT_WARN = 50


def _colored(text: str, color: str) -> str:
    """Wrap *text* in Rich markup without affecting visible width calculations."""
    return f"[{color}]{text}[/{color}]"


def _freshness_parts(last_ms: int | None) -> tuple[str, str]:
    """Return (visible_text, color) for a last-updated timestamp."""
    if not last_ms:
        return ("unknown", "dim")
    import time

    age_s = time.time() - (last_ms / 1000)
    days = int(age_s / 86400)
    if days >= _STALE_DAYS_CRIT:
        return (f"{days}d ago", "red")
    if days >= _STALE_DAYS_WARN:
        return (f"{days}d ago", "yellow")
    if days >= 1:
        return (f"{days}d ago", "green")
    hours = int(age_s / 3600)
    if hours >= 1:
        return (f"{hours}h ago", "green")
    mins = max(1, int(age_s / 60))
    return (f"{mins}m ago", "green")


class CatalogOverviewPanel(Static):
    """Shows namespace or warehouse-level stats -- metadata only, no heavy IO."""

    def show_namespace(self, config: CatalogConfig, namespace: str, table_ids: list[str]) -> None:
        self.loading = True
        self.run_worker(self._load_namespace(config, namespace, table_ids), exclusive=True)

    def show_warehouse(self, config: CatalogConfig, tables_by_ns: dict[str, list[str]]) -> None:
        self.loading = True
        self.run_worker(self._load_warehouse(config, tables_by_ns), exclusive=True)

    async def _load_namespace(
        self, config: CatalogConfig, namespace: str, table_ids: list[str]
    ) -> None:
        try:
            data = await asyncio.to_thread(
                partial(collect_namespace_overview, config, namespace, table_ids)
            )
            self.update(self._format_namespace(data))
        except Exception as exc:
            self.update(f"[red]Error loading namespace overview: {exc}[/red]")
            self.app.notify(str(exc), severity="error")
        finally:
            self.loading = False

    async def _load_warehouse(
        self, config: CatalogConfig, tables_by_ns: dict[str, list[str]]
    ) -> None:
        try:
            data = await asyncio.to_thread(
                partial(collect_warehouse_overview, config, tables_by_ns)
            )
            self.update(self._format_warehouse(data))
        except Exception as exc:
            self.update(f"[red]Error loading warehouse overview: {exc}[/red]")
            self.app.notify(str(exc), severity="error")
        finally:
            self.loading = False

    # -- formatting helpers --------------------------------------------------

    @staticmethod
    def _short_name(t: dict[str, Any]) -> str:
        n: str = t["name"]
        return n.split(".")[-1] if "." in n else n

    @staticmethod
    def _full_name(t: dict[str, Any]) -> str:
        ns: str = t.get("namespace", "")
        name: str = t["name"]
        short = name.split(".")[-1] if "." in name else name
        return f"{ns}.{short}" if ns else short

    def _format_namespace(self, data: dict) -> str:
        lines: list[str] = []
        ns = data["namespace"]
        count = data["table_count"]
        lines.append(
            f"[bold]Namespace:[/bold] [cyan]{ns}[/cyan]  ({count} table{'s' if count != 1 else ''})"
        )

        total_size = data.get("total_size", 0)
        total_files = data.get("total_files", 0)
        total_records = data.get("total_records", 0)
        if total_size or total_files or total_records:
            lines.append(
                f"  {format_bytes(total_size)}  |  "
                f"{total_files:,} files  |  "
                f"{total_records:,} records"
            )
        lines.append("")

        # -- table list -------------------------------------------------------
        lines.append("[bold]Tables[/bold]  [dim](sorted by last update, oldest first)[/dim]")
        lines.append("")
        if data["tables"]:
            W_NAME = 22
            W_UPD = 10
            W_SIZE = 10
            W_FILES = 6
            W_ROWS = 10
            W_SNAPS = 6
            W_VER = 3
            hdr = (
                f"  {'Name':<{W_NAME}}  {'Updated':>{W_UPD}}  "
                f"{'Size':>{W_SIZE}}  {'Files':>{W_FILES}}  "
                f"{'Rows':>{W_ROWS}}  {'Snaps':>{W_SNAPS}}  "
                f"{'Ver':>{W_VER}}  {'Partition'}"
            )
            lines.append(f"  [dim]{hdr.strip()}[/dim]")
            for t in data["tables"]:
                name = self._short_name(t)[:W_NAME]
                upd_text, upd_color = _freshness_parts(t["last_updated_ms"])
                sc = t["snapshot_count"]
                snap_text = str(sc)
                snap_color = "yellow" if sc >= _SNAPSHOT_WARN else ""
                padded = snap_text.rjust(W_SNAPS)
                snap_col = _colored(padded, snap_color) if snap_color else padded
                lines.append(
                    f"  [bold]{name:<{W_NAME}}[/bold]  "
                    f"{_colored(upd_text.rjust(W_UPD), upd_color)}  "
                    f"{format_bytes(t['total_size']).rjust(W_SIZE)}  "
                    f"{str(t['total_files']).rjust(W_FILES)}  "
                    f"{self._compact_number(t['total_records']).rjust(W_ROWS)}  "
                    f"{snap_col}  "
                    f"v{t['format_version']}  "
                    f"[dim]{t['partition']}[/dim]"
                )
        else:
            lines.append("  [dim]No tables in this namespace[/dim]")

        # -- alerts -----------------------------------------------------------
        stale = [t for t in data["tables"] if self._is_stale(t["last_updated_ms"])]
        if stale:
            lines.append("")
            lines.append(
                f"[yellow]⚠ {len(stale)} table{'s' if len(stale) != 1 else ''} "
                f"not updated in over {_STALE_DAYS_WARN} days — "
                f"may indicate stale pipelines[/yellow]"
            )

        snap_heavy = [t for t in data["tables"] if t["snapshot_count"] >= _SNAPSHOT_WARN]
        if snap_heavy:
            lines.append("")
            names = ", ".join(self._short_name(t) for t in snap_heavy)
            lines.append(
                f"[yellow]⚠ High snapshot count on {names} — "
                f"expiring old snapshots would reduce metadata overhead[/yellow]"
            )

        # -- schema consistency -----------------------------------------------
        conflicts = data.get("schema_conflicts", [])
        if conflicts:
            lines.append("")
            lines.append(
                "[bold]Schema Conflicts[/bold]  "
                "[dim](same field name, different type across tables)[/dim]"
            )
            for c in conflicts:
                parts = []
                for td in c["types"]:
                    tbls = ", ".join(td["tables"])
                    parts.append(f"[cyan]{td['type']}[/cyan] in {tbls}")
                lines.append(f"  [yellow]{c['field']}[/yellow]  →  {' | '.join(parts)}")

        if data["errors"]:
            lines.append("")
            lines.append(f"[red]{len(data['errors'])} table(s) failed to load[/red]")

        return "\n".join(lines)

    def _format_warehouse(self, data: dict) -> str:
        lines: list[str] = []

        # -- headline ---------------------------------------------------------
        lines.append(
            f"[bold]Warehouse Overview[/bold]  "
            f"{data['namespace_count']} namespace{'s' if data['namespace_count'] != 1 else ''}, "
            f"{data['total_tables']} table{'s' if data['total_tables'] != 1 else ''}"
        )
        total_size = data.get("total_size", 0)
        total_files = data.get("total_files", 0)
        total_records = data.get("total_records", 0)
        if total_size or total_files or total_records:
            lines.append(
                f"  {format_bytes(total_size)}  |  "
                f"{total_files:,} files  |  "
                f"{total_records:,} records"
            )
        lines.append("")

        # -- format versions --------------------------------------------------
        fv = data["format_versions"]
        if fv:
            parts = [f"v{v}: {c}" for v, c in sorted(fv.items())]
            lines.append(f"[bold]Format Versions[/bold]  {', '.join(parts)}")
            v1_count = fv.get(1, 0)
            if v1_count:
                lines.append(
                    f"  [yellow]{v1_count} table{'s' if v1_count != 1 else ''} still on v1 — "
                    f"upgrading to v2 enables row-level deletes and better stats[/yellow]"
                )
            lines.append("")

        # -- namespace comparison ---------------------------------------------
        ns_list = data.get("namespaces", [])
        if ns_list:
            lines.append("[bold]Namespaces[/bold]")
            lines.append("")
            W_NS = 25
            W_T = 7
            W_S = 10
            W_F = 7
            W_R = 12
            hdr = (
                f"  {'Namespace':<{W_NS}}  {'Tables':>{W_T}}  "
                f"{'Size':>{W_S}}  {'Files':>{W_F}}  {'Rows':>{W_R}}"
            )
            lines.append(f"  [dim]{hdr.strip()}[/dim]")
            for ns in sorted(ns_list, key=lambda n: n.get("total_size", 0), reverse=True):
                lines.append(
                    f"  {ns['namespace']:<{W_NS}}  "
                    f"{str(ns['table_count']).rjust(W_T)}  "
                    f"{format_bytes(ns.get('total_size', 0)).rjust(W_S)}  "
                    f"{str(ns.get('total_files', 0)).rjust(W_F)}  "
                    f"{self._compact_number(ns.get('total_records', 0)).rjust(W_R)}"
                )
            lines.append("")

        # -- stalest tables ---------------------------------------------------
        lines.append(
            "[bold]Stalest Tables[/bold]  "
            "[dim](oldest first — may indicate broken or retired pipelines)[/dim]"
        )
        lines.append("")
        if data["stalest_tables"]:
            W_TBL = 35
            W_UPD = 10
            W_SNAPS = 6
            hdr = f"  {'Table':<{W_TBL}}  {'Updated':>{W_UPD}}  {'Snaps':>{W_SNAPS}}  Partition"
            lines.append(f"  [dim]{hdr.strip()}[/dim]")
            for t in data["stalest_tables"]:
                full = self._full_name(t)[:W_TBL]
                upd_text, upd_color = _freshness_parts(t["last_updated_ms"])
                sc = t["snapshot_count"]
                snap_text = str(sc)
                snap_color = "yellow" if sc >= _SNAPSHOT_WARN else ""
                padded = snap_text.rjust(W_SNAPS)
                snap_col = _colored(padded, snap_color) if snap_color else padded
                lines.append(
                    f"  {full:<{W_TBL}}  "
                    f"{_colored(upd_text.rjust(W_UPD), upd_color)}  "
                    f"{snap_col}  "
                    f"[dim]{t['partition']}[/dim]"
                )
        else:
            lines.append("  [dim]No tables found[/dim]")

        # -- most snapshots ---------------------------------------------------
        lines.append("")
        lines.append(
            "[bold]Most Snapshots[/bold]  "
            "[dim](snapshot accumulation increases metadata size)[/dim]"
        )
        lines.append("")
        if data["most_snapshots"]:
            W_TBL = 35
            W_SNAPS = 6
            W_UPD = 10
            hdr = f"  {'Table':<{W_TBL}}  {'Snaps':>{W_SNAPS}}  {'Updated':>{W_UPD}}"
            lines.append(f"  [dim]{hdr.strip()}[/dim]")
            for t in data["most_snapshots"]:
                full = self._full_name(t)[:W_TBL]
                upd_text, upd_color = _freshness_parts(t["last_updated_ms"])
                sc = t["snapshot_count"]
                snap_text = str(sc)
                snap_color = "yellow" if sc >= _SNAPSHOT_WARN else ""
                padded = snap_text.rjust(W_SNAPS)
                snap_col = _colored(padded, snap_color) if snap_color else padded
                lines.append(
                    f"  {full:<{W_TBL}}  {snap_col}  {_colored(upd_text.rjust(W_UPD), upd_color)}"
                )
        else:
            lines.append("  [dim]No tables found[/dim]")

        if data["errors"]:
            lines.append("")
            lines.append(f"[red]{len(data['errors'])} table(s) failed to load[/red]")

        return "\n".join(lines)

    @staticmethod
    def _compact_number(n: int) -> str:
        if n >= 1_000_000_000:
            return f"{n / 1_000_000_000:.1f}B"
        if n >= 1_000_000:
            return f"{n / 1_000_000:.1f}M"
        if n >= 1_000:
            return f"{n / 1_000:.1f}K"
        return str(n)

    @staticmethod
    def _is_stale(last_ms: int | None) -> bool:
        if not last_ms:
            return True
        import time

        age_s = time.time() - (last_ms / 1000)
        return age_s > (_STALE_DAYS_WARN * 86400)
