"""Modal screens for the iceberg-meta TUI."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Static

from iceberg_meta.catalog import CatalogConfig, get_table
from iceberg_meta.formatters import (
    _find_snapshot,
    collect_diff,
    collect_files,
    collect_manifests,
)
from iceberg_meta.utils import format_bytes, format_timestamp_ms, truncate_path


class DiffScreen(ModalScreen[None]):
    """Modal showing the diff between two snapshots."""

    BINDINGS = [("escape", "dismiss", "Close")]  # noqa: RUF012

    def __init__(
        self,
        config: CatalogConfig,
        table_id: str,
        snap_id_1: int,
        snap_id_2: int,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.config = config
        self.table_id = table_id
        self.snap_id_1 = snap_id_1
        self.snap_id_2 = snap_id_2

    def compose(self) -> ComposeResult:
        with Vertical(id="diff-container") as container:
            container.border_title = f"Diff  [dim]│[/dim]  {self.snap_id_1} → {self.snap_id_2}"
            yield Static("Loading diff...", id="diff-content")
            yield Button("Close  [dim](esc)[/dim]", id="diff-close", variant="primary")

    def on_mount(self) -> None:
        self.run_worker(self._load())

    async def _load(self) -> None:
        try:
            tbl = get_table(self.config, self.table_id)
            data = collect_diff(tbl, self.snap_id_1, self.snap_id_2)

            lines: list[str] = []
            lines.append(
                f"[bold]Snapshot {data['snap_id_1']} → {data['snap_id_2']}[/bold]  "
                f"[yellow]({data['operation']})[/yellow]\n"
            )
            lines.append(
                f"  [green]+ {data['added_count']} files added[/green]  "
                f"({format_bytes(data['added_size'])}, {data['added_rows']:,} rows)"
            )
            lines.append(
                f"  [red]- {data['deleted_count']} files deleted[/red]  "
                f"({format_bytes(data['deleted_size'])}, {data['deleted_rows']:,} rows)"
            )

            sign = "+" if data["net_size"] >= 0 else ""
            lines.append(
                f"\n  [bold]Net:[/bold] {sign}{data['net_files']} files, "
                f"{sign}{format_bytes(data['net_size'])}, {sign}{data['net_rows']:,} rows"
            )

            if data["added_files"]:
                lines.append("\n[bold green]Added:[/bold green]")
                for f in data["added_files"]:
                    lines.append(
                        f"  {f['file_path']}  {f['record_count']:,} rows  "
                        f"{format_bytes(f['file_size'])}"
                    )

            if data["deleted_files"]:
                lines.append("\n[bold red]Deleted:[/bold red]")
                for f in data["deleted_files"]:
                    lines.append(
                        f"  {f['file_path']}  {f['record_count']:,} rows  "
                        f"{format_bytes(f['file_size'])}"
                    )

            content = self.query_one("#diff-content", Static)
            content.update("\n".join(lines))
        except Exception as exc:
            content = self.query_one("#diff-content", Static)
            content.update(f"[red]Failed to compute diff: {exc}[/red]")
            self.app.notify(f"Diff error: {exc}", severity="error")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "diff-close":
            self.dismiss()


class SnapshotDetailScreen(ModalScreen[None]):
    """Modal showing a deep-dive into a single snapshot."""

    BINDINGS = [("escape", "dismiss", "Close")]  # noqa: RUF012

    def __init__(
        self,
        config: CatalogConfig,
        table_id: str,
        snapshot_id: int,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.config = config
        self.table_id = table_id
        self.snapshot_id = snapshot_id

    def compose(self) -> ComposeResult:
        with Vertical(id="diff-container") as container:
            container.border_title = f"Snapshot Detail  [dim]│[/dim]  {self.snapshot_id}"
            yield Static("Loading snapshot detail...", id="diff-content")
            yield Button("Close  [dim](esc)[/dim]", id="diff-close", variant="primary")

    def on_mount(self) -> None:
        self.run_worker(self._load())

    async def _load(self) -> None:
        try:
            tbl = get_table(self.config, self.table_id)
            snap = _find_snapshot(tbl, self.snapshot_id)
            if not snap:
                content = self.query_one("#diff-content", Static)
                content.update(f"[red]Snapshot {self.snapshot_id} not found[/red]")
                return

            lines: list[str] = []
            op = snap.summary.operation.value if snap.summary else "N/A"
            lines.append(f"[bold]Snapshot {snap.snapshot_id}[/bold]  [yellow]{op}[/yellow]\n")
            lines.append(f"  [cyan]Timestamp:[/cyan]     {format_timestamp_ms(snap.timestamp_ms)}")
            lines.append(f"  [cyan]Parent:[/cyan]        {snap.parent_snapshot_id or 'None'}")
            lines.append(f"  [cyan]Schema ID:[/cyan]     {snap.schema_id or 'N/A'}")
            lines.append(f"  [cyan]Manifest List:[/cyan] {truncate_path(snap.manifest_list)}")

            if snap.summary:
                lines.append("\n[bold]Summary Properties[/bold]")
                for k, v in snap.summary.additional_properties.items():
                    lines.append(f"  [dim]{k}:[/dim] {v}")

            manifest_data = collect_manifests(tbl, snapshot_id=self.snapshot_id)
            if manifest_data:
                lines.append(f"\n[bold]Manifests[/bold] ({len(manifest_data)})")
                for m in manifest_data:
                    lines.append(
                        f"  {m['Path']}  {m['Length']}  "
                        f"[green]+{m['Added']}[/green] [dim]existing:{m['Existing']}[/dim] "
                        f"[red]-{m['Deleted']}[/red]"
                    )

            file_data = collect_files(tbl, snapshot_id=self.snapshot_id)
            if file_data:
                total_size = sum(r.get("file_size_raw", 0) for r in file_data)
                total_rows = sum(r.get("record_count_raw", 0) for r in file_data)
                lines.append(
                    f"\n[bold]Data Files[/bold] ({len(file_data)} files, "
                    f"{format_bytes(total_size)}, {total_rows:,} rows)"
                )
                for f in file_data[:20]:
                    lines.append(f"  {f['File Path']}  {f['Record Count']} rows  {f['File Size']}")
                if len(file_data) > 20:
                    lines.append(f"  [dim]... and {len(file_data) - 20} more files[/dim]")

            content = self.query_one("#diff-content", Static)
            content.update("\n".join(lines))
        except Exception as exc:
            content = self.query_one("#diff-content", Static)
            content.update(f"[red]Failed to load snapshot detail: {exc}[/red]")
            self.app.notify(f"Snapshot detail error: {exc}", severity="error")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "diff-close":
            self.dismiss()


class HelpScreen(ModalScreen[None]):
    """Modal showing all keybindings and available features."""

    BINDINGS = [("escape", "dismiss", "Close")]  # noqa: RUF012

    def compose(self) -> ComposeResult:
        with Vertical(id="diff-container") as container:
            container.border_title = "Help"
            yield Static(self._build_help(), id="diff-content")
            yield Button("Close  [dim](esc)[/dim]", id="diff-close", variant="primary")

    def _build_help(self) -> str:
        lines = [
            "[bold underline]iceberg-meta TUI -- Help[/bold underline]\n",
            "[bold]Navigation[/bold]",
            "  [cyan]Arrow keys[/cyan]       Navigate sidebar and tables",
            "  [cyan]Enter[/cyan]            Select a table from the sidebar",
            "  [cyan]Tab / Shift+Tab[/cyan]  Move between panels",
            "",
            "[bold]Tab Shortcuts[/bold]",
            "  [cyan]1[/cyan]  Summary      Table overview, recent ops, file health",
            "  [cyan]2[/cyan]  Snapshots    Snapshot history with operations",
            "  [cyan]3[/cyan]  Schema       Schema evolution with diffs between versions",
            "  [cyan]4[/cyan]  Files        Data files with size distribution stats",
            "  [cyan]5[/cyan]  Manifests    Manifest files for current snapshot",
            "  [cyan]6[/cyan]  Health       Comprehensive health report:",
            "              file sizes, small-file detection,",
            "              delete file accumulation, partition skew,",
            "              column null rates, column sizes, column bounds",
            "  [cyan]7[/cyan]  Tree         Full metadata hierarchy",
            "",
            "[bold]Actions[/bold]",
            "  [cyan]d[/cyan]  Diff the two most recent snapshots",
            "  [cyan]s[/cyan]  Snapshot detail (deep-dive on selected snapshot)",
            "  [cyan]r[/cyan]  Refresh all panels",
            "  [cyan]?[/cyan]  Show this help screen",
            "  [cyan]q[/cyan]  Quit",
            "",
            "[bold]CLI Equivalents[/bold]",
            "",
            "  [dim]Tab/Action[/dim]     [dim]CLI Command[/dim]",
            "  Sidebar        iceberg-meta list-tables",
            "  Summary        iceberg-meta summary <table>",
            "  Snapshots      iceberg-meta snapshots <table>",
            "  Schema         iceberg-meta schema <table> --history",
            "  Files          iceberg-meta files <table>",
            "  Manifests      iceberg-meta manifests <table>",
            "  Health         iceberg-meta health <table>",
            "  Tree           iceberg-meta tree <table>",
            "  Diff (d)       iceberg-meta diff <table> <snap1> <snap2>",
            "  Detail (s)     iceberg-meta snapshot-detail <table> <snap-id>",
            "",
            "[bold]CLI-only Features[/bold]",
            "  iceberg-meta init                  Interactive config setup",
            "  iceberg-meta table-info <table>     UUID, location, properties, partition spec",
            "  iceberg-meta partitions <table>     Partition stats (basic table view)",
            "  iceberg-meta snapshots --watch N    Live-watch mode",
            "  iceberg-meta -o json|csv ...        Machine-readable output",
        ]
        return "\n".join(lines)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "diff-close":
            self.dismiss()
