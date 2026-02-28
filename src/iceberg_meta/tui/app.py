"""Iceberg-meta interactive terminal UI."""

from __future__ import annotations

from pathlib import Path

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Footer, Static, TabbedContent, TabPane

from iceberg_meta.catalog import CatalogConfig
from iceberg_meta.tui.screens import DiffScreen, HelpScreen, SnapshotDetailScreen
from iceberg_meta.tui.widgets import (
    CatalogOverviewPanel,
    FilesPanel,
    HealthPanel,
    ManifestsPanel,
    MetadataTreePanel,
    SchemaPanel,
    SnapshotsPanel,
    SummaryPanel,
    TableBrowser,
)

CSS_PATH = Path(__file__).parent / "app.tcss"


class IcebergMetaApp(App):
    """Interactive TUI for exploring Iceberg table metadata."""

    TITLE = "iceberg-meta"
    SUB_TITLE = "Iceberg Metadata Explorer"
    CSS_PATH = CSS_PATH

    BINDINGS = [  # noqa: RUF012
        ("q", "quit", "Quit"),
        ("r", "reload", "Refresh"),
        ("d", "diff", "Diff"),
        ("s", "snapshot_detail", "Detail"),
        ("question_mark", "help", "Help"),
        ("1", "tab('summary')", "Summary"),
        ("2", "tab('snapshots')", "Snapshots"),
        ("3", "tab('schema')", "Schema"),
        ("4", "tab('files')", "Files"),
        ("5", "tab('manifests')", "Manifests"),
        ("6", "tab('health')", "Health"),
        ("7", "tab('tree')", "Tree"),
    ]

    def __init__(self, catalog_config: CatalogConfig, **kwargs) -> None:
        super().__init__(**kwargs)
        self.catalog_config = catalog_config
        self._current_table: str | None = None

    def compose(self) -> ComposeResult:
        yield Static("iceberg-meta", id="app-title")
        with Horizontal():
            with Vertical(id="sidebar"):
                yield TableBrowser(self.catalog_config, id="browser")
            with Vertical(id="detail"):
                yield Static(
                    "[dim]Select a table from the sidebar[/dim]\n\n"
                    "[dim]Use arrow keys to navigate, Enter to select[/dim]\n\n"
                    "[dim]Press [bold]?[/bold] for help[/dim]",
                    id="no-table",
                )
                yield CatalogOverviewPanel(id="overview-panel")
                with TabbedContent(id="tabs"):
                    with TabPane("Summary", id="summary"):
                        yield SummaryPanel(id="summary-panel")
                    with TabPane("Snapshots", id="snapshots"):
                        yield SnapshotsPanel(id="snapshots-panel")
                    with TabPane("Schema", id="schema"):
                        yield SchemaPanel(id="schema-panel")
                    with TabPane("Files", id="files"):
                        yield FilesPanel(id="files-panel")
                    with TabPane("Manifests", id="manifests"):
                        yield ManifestsPanel(id="manifests-panel")
                    with TabPane("Health", id="health"):
                        yield HealthPanel(id="health-panel")
                    with TabPane("Tree", id="tree"):
                        yield MetadataTreePanel(id="tree-panel")
        yield Footer()

    def on_mount(self) -> None:
        sidebar = self.query_one("#sidebar")
        sidebar.border_title = "Catalog"
        no_table = self.query_one("#no-table", Static)
        no_table.border_title = "Detail"
        tabs = self.query_one("#tabs", TabbedContent)
        tabs.display = False
        overview = self.query_one("#overview-panel", CatalogOverviewPanel)
        overview.display = False

    def _show_detail_view(self, mode: str) -> None:
        """Toggle between 'empty', 'table', and 'overview' detail views."""
        self.query_one("#no-table", Static).display = mode == "empty"
        self.query_one("#overview-panel", CatalogOverviewPanel).display = mode == "overview"
        self.query_one("#tabs", TabbedContent).display = mode == "table"

    def on_table_browser_table_selected(self, event: TableBrowser.TableSelected) -> None:
        self._current_table = event.table_id

        title_bar = self.query_one("#app-title", Static)
        title_bar.update(f"iceberg-meta  [dim]│[/dim]  {event.table_id}")

        self._show_detail_view("table")
        self._load_all_panels()

    def on_table_browser_namespace_selected(self, event: TableBrowser.NamespaceSelected) -> None:
        self._current_table = None
        title_bar = self.query_one("#app-title", Static)
        title_bar.update(f"iceberg-meta  [dim]│[/dim]  [cyan]{event.namespace}[/cyan]")

        self._show_detail_view("overview")
        overview = self.query_one("#overview-panel", CatalogOverviewPanel)
        overview.border_title = f"Namespace: {event.namespace}"
        overview.show_namespace(self.catalog_config, event.namespace, event.table_ids)

    def on_table_browser_catalog_root_selected(
        self, event: TableBrowser.CatalogRootSelected
    ) -> None:
        self._current_table = None
        title_bar = self.query_one("#app-title", Static)
        title_bar.update("iceberg-meta  [dim]│[/dim]  [cyan]Warehouse Overview[/cyan]")

        self._show_detail_view("overview")
        overview = self.query_one("#overview-panel", CatalogOverviewPanel)
        overview.border_title = "Warehouse"
        overview.show_warehouse(self.catalog_config, event.tables_by_ns)

    def _load_all_panels(self) -> None:
        if not self._current_table:
            return
        config = self.catalog_config
        table_id = self._current_table

        self.query_one("#summary-panel", SummaryPanel).update_table(config, table_id)
        self.query_one("#snapshots-panel", SnapshotsPanel).update_table(config, table_id)
        self.query_one("#schema-panel", SchemaPanel).update_table(config, table_id)
        self.query_one("#files-panel", FilesPanel).update_table(config, table_id)
        self.query_one("#manifests-panel", ManifestsPanel).update_table(config, table_id)
        self.query_one("#health-panel", HealthPanel).update_table(config, table_id)
        self.query_one("#tree-panel", MetadataTreePanel).update_table(config, table_id)

    def action_reload(self) -> None:
        browser = self.query_one("#browser", TableBrowser)
        browser.run_worker(browser._load_tables(), exclusive=True)
        self._load_all_panels()
        self.notify("Refreshed")

    def action_tab(self, tab_id: str) -> None:
        tabs = self.query_one("#tabs", TabbedContent)
        if tabs.display:
            tabs.active = tab_id

    def action_help(self) -> None:
        self.push_screen(HelpScreen())

    def action_diff(self) -> None:
        if not self._current_table:
            self.notify("Select a table first", severity="warning")
            return

        snapshots_panel = self.query_one("#snapshots-panel", SnapshotsPanel)
        dt = snapshots_panel.query_one("#snapshots-dt")

        if dt.row_count < 2:
            self.notify("Need at least 2 snapshots to diff", severity="warning")
            return

        rows = list(dt.rows)
        if len(rows) < 2:
            return

        first_row = dt.get_row(rows[0])
        second_row = dt.get_row(rows[1])

        if dt.cursor_row is not None and dt.cursor_row < len(rows):
            cursor_key = rows[dt.cursor_row]
            selected_row = dt.get_row(cursor_key)
            snap2_id = int(selected_row[0])
            prev_idx = max(0, dt.cursor_row - 1)
            prev_row = dt.get_row(rows[prev_idx])
            snap1_id = int(prev_row[0])
            if snap1_id == snap2_id and len(rows) >= 2:
                snap1_id = int(first_row[0])
                snap2_id = int(second_row[0])
        else:
            snap1_id = int(first_row[0])
            snap2_id = int(second_row[0])

        if snap1_id > snap2_id:
            snap1_id, snap2_id = snap2_id, snap1_id

        self.push_screen(DiffScreen(self.catalog_config, self._current_table, snap1_id, snap2_id))

    def action_snapshot_detail(self) -> None:
        if not self._current_table:
            self.notify("Select a table first", severity="warning")
            return

        snapshots_panel = self.query_one("#snapshots-panel", SnapshotsPanel)
        dt = snapshots_panel.query_one("#snapshots-dt")

        if dt.row_count == 0:
            self.notify("No snapshots available", severity="warning")
            return

        rows = list(dt.rows)
        if dt.cursor_row is not None and dt.cursor_row < len(rows):
            cursor_key = rows[dt.cursor_row]
            selected_row = dt.get_row(cursor_key)
            snap_id = int(selected_row[0])
        else:
            first_row = dt.get_row(rows[0])
            snap_id = int(first_row[0])

        self.push_screen(SnapshotDetailScreen(self.catalog_config, self._current_table, snap_id))
