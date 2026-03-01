"""Iceberg Metadata Explorer CLI."""

from __future__ import annotations

import shutil
import time
from pathlib import Path

import typer
from dotenv import load_dotenv
from pyiceberg.exceptions import (
    ForbiddenError,
    NoSuchNamespaceError,
    NoSuchTableError,
    NotInstalledError,
    ServerError,
    SignError,
    UnauthorizedError,
)
from rich.console import Console

from iceberg_meta import __version__, formatters
from iceberg_meta.catalog import (
    CONFIG_FILE,
    CatalogConfig,
    get_table,
    list_all_tables,
    merge_config_file,
    resolve_catalog_config,
    test_connection,
)
from iceberg_meta.output import OutputFormat
from iceberg_meta.utils import format_timestamp_ms


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"iceberg-meta {__version__}")
        raise typer.Exit()


app = typer.Typer(
    name="iceberg-meta",
    help="Explore Apache Iceberg table metadata.",
    no_args_is_help=True,
)
console = Console()
err_console = Console(stderr=True)


# ─── error handling ───────────────────────────────────────────


def _friendly_error(e: Exception, config: CatalogConfig, table: str | None = None) -> None:
    """Print a friendly error message instead of a raw traceback."""
    msg = str(e).lower()
    etype = type(e).__name__.lower()

    if isinstance(e, NoSuchTableError) or ("table" in msg and "does not exist" in msg):
        err_console.print(
            f"[red bold]Table not found:[/red bold] '{table or 'unknown'}'\n"
            "  Run [bold]iceberg-meta list-tables[/bold] to see available tables."
        )
    elif isinstance(e, NoSuchNamespaceError) or "nosuchnamespace" in etype:
        err_console.print(
            "[red bold]Namespace not found.[/red bold]\n"
            "  Run [bold]iceberg-meta list-tables[/bold] to see available namespaces."
        )
    elif (
        isinstance(e, (UnauthorizedError, ForbiddenError)) or "unauthorized" in msg or "401" in msg
    ):
        err_console.print(
            "[red bold]Authentication failed:[/red bold] Credentials are invalid or expired.\n"
            "  Check your access key, secret key, and any session tokens."
        )
    elif "access denied" in msg or "forbidden" in msg or "403" in msg:
        err_console.print(
            "[red bold]Access denied:[/red bold] S3 credentials may be invalid.\n"
            "  Check s3.access-key-id and s3.secret-access-key in your config."
        )
    elif isinstance(e, SignError) or "signature" in msg or "signerror" in etype or "signing" in msg:
        err_console.print(
            "[red bold]S3 signing error:[/red bold] Request signing failed.\n"
            "  Check your AWS credentials and region (s3.region in config)."
        )
    elif isinstance(e, NotInstalledError) or "notinstalled" in etype or "no module named" in msg:
        err_console.print(
            f"[red bold]Missing dependency:[/red bold] {e}\n"
            "  Install the required optional dependency and try again."
        )
    elif isinstance(e, ServerError) or "500" in msg or "server error" in msg:
        err_console.print(
            "[red bold]Server error:[/red bold] The catalog server returned an internal error.\n"
            "  Check the catalog server logs for details."
        )
    elif "could not connect" in msg or "connection" in msg or "urlopen" in msg or "refused" in msg:
        uri = config.properties.get("uri", "unknown")
        err_console.print(
            f"[red bold]Connection failed:[/red bold] Could not connect to catalog at {uri}\n"
            "  Is the catalog running? Try: docker compose up -d"
        )
    elif "timeout" in msg or "timed out" in msg:
        uri = config.properties.get("uri", "unknown")
        err_console.print(
            f"[red bold]Connection timed out:[/red bold] Catalog at {uri} did not respond.\n"
            "  Is the catalog reachable? Check your network and URI."
        )
    elif "unable to open database" in msg or "no such table" in msg:
        uri = config.properties.get("uri", "unknown")
        err_console.print(
            f"[red bold]Database error:[/red bold] Could not open catalog database.\n"
            f"  URI: {uri}\n"
            "  If using SQLite, make sure the database file exists.\n"
            "  Try: [bold]make infra-up && make seed[/bold]"
        )
    elif "database is locked" in msg:
        err_console.print(
            "[red bold]Database locked:[/red bold] Another process is using the catalog database.\n"
            "  Wait a moment and try again, or check for other running processes."
        )
    elif "snapshot" in msg and "not found" in msg:
        err_console.print(
            f"[red bold]Snapshot not found:[/red bold] {e}\n"
            "  Run [bold]iceberg-meta snapshots <table>[/bold] to see valid snapshot IDs."
        )
    elif "nosuch" in msg or "not found" in msg or ("table" in msg and "does not exist" in msg):
        err_console.print(
            f"[red bold]Table not found:[/red bold] '{table or 'unknown'}'\n"
            "  Run [bold]iceberg-meta list-tables[/bold] to see available tables."
        )
    elif "nosuchnamespace" in etype or ("namespace" in msg and "not found" in msg):
        err_console.print(
            "[red bold]Namespace not found.[/red bold]\n"
            "  Run [bold]iceberg-meta list-tables[/bold] to see available namespaces."
        )
    elif "unauthorized" in msg or "401" in msg or "unauthorizederror" in etype:
        err_console.print(
            "[red bold]Authentication failed:[/red bold] Credentials are invalid or expired.\n"
            "  Check your access key, secret key, and any session tokens."
        )
    elif "access denied" in msg or "forbidden" in msg or "403" in msg:
        err_console.print(
            "[red bold]Access denied:[/red bold] S3 credentials may be invalid.\n"
            "  Check s3.access-key-id and s3.secret-access-key in your config."
        )
    elif "signature" in msg or "signerror" in etype or "signing" in msg:
        err_console.print(
            "[red bold]S3 signing error:[/red bold] Request signing failed.\n"
            "  Check your AWS credentials and region (s3.region in config)."
        )
    elif "notinstalled" in etype or "no module named" in msg:
        err_console.print(
            f"[red bold]Missing dependency:[/red bold] {e}\n"
            "  Install the required optional dependency and try again."
        )
    elif (
        "invalid" in msg and ("table" in msg or "identifier" in msg or "does not contain" in msg)
    ) or "empty namespace" in msg:
        err_console.print(
            f"[red bold]Invalid table name:[/red bold] '{table or 'unknown'}'\n"
            "  Use the format [bold]namespace.table_name[/bold]"
        )
    elif "500" in msg or "server error" in msg or "internal server error" in msg:
        err_console.print(
            "[red bold]Server error:[/red bold] The catalog server returned an internal error.\n"
            "  Check the catalog server logs for details."
        )
    elif "yaml" in etype:
        err_console.print(
            f"[red bold]Config error:[/red bold] Invalid YAML in your config file.\n  {e}"
        )
    elif not config.properties:
        err_console.print(
            "[red bold]No catalog configured.[/red bold]\n"
            "  Run [bold]iceberg-meta init[/bold] to set up a catalog, or pass --uri."
        )
    else:
        err_console.print(f"[red bold]Error:[/red bold] {e}")
    raise SystemExit(1)


def _run(fn, config: CatalogConfig, table: str | None = None):
    """Execute *fn* with friendly error handling."""
    try:
        return fn()
    except SystemExit:
        raise
    except Exception as e:
        _friendly_error(e, config, table)


# ─── global callback ─────────────────────────────────────────


@app.callback()
def main(
    ctx: typer.Context,
    catalog_name: str | None = typer.Option(
        None, "--catalog", "-c", help="Catalog name (from config file)"
    ),
    catalog_uri: str | None = typer.Option(None, "--uri", help="Catalog URI"),
    warehouse: str | None = typer.Option(None, "--warehouse", "-w", help="Warehouse path"),
    output: OutputFormat = typer.Option(  # noqa: B008
        OutputFormat.TABLE, "--output", "-o", help="Output format (table, json, csv)"
    ),
    env_file: Path | None = typer.Option(  # noqa: B008
        None,
        "--env-file",
        "-e",
        help="Path to .env file to load",
        exists=True,
        dir_okay=False,
    ),
    version: bool | None = typer.Option(
        None,
        "--version",
        "-V",
        help="Show version and exit",
        callback=_version_callback,
        is_eager=True,
    ),
):
    """Explore Apache Iceberg table metadata."""
    if env_file:
        load_dotenv(env_file, override=True)
    else:
        load_dotenv()

    ctx.ensure_object(dict)

    invoked = ctx.invoked_subcommand
    if invoked in ("init", "doctor", "demo", "quickstart"):
        ctx.obj["output_format"] = output
        return

    try:
        ctx.obj["catalog_config"] = resolve_catalog_config(
            catalog_name=catalog_name,
            catalog_uri=catalog_uri,
            warehouse=warehouse,
        )
    except ValueError as exc:
        exc_msg = str(exc)
        err_console.print(f"[red bold]Config error:[/red bold] {exc_msg}")

        if "referenced in config but not set" in exc_msg:
            err_console.print(
                "\n  Your config file uses ${{VAR}} placeholders that need matching\n"
                "  environment variables.\n\n"
                "  Option 1 -- place a .env file in your working directory:\n"
                "    iceberg-meta picks up .env automatically (no 'source' needed)\n\n"
                "  Option 2 -- point to a specific .env file:\n"
                "    [bold]iceberg-meta --env-file path/to/.env list-tables[/bold]\n\n"
                "  Option 3 -- export variables in your shell:\n"
                "    [bold]export ICEBERG_CATALOG_URI=...[/bold]\n"
                "    [bold]export AWS_ACCESS_KEY_ID=...[/bold]\n\n"
                "  Run [bold]iceberg-meta doctor[/bold] to diagnose further."
            )
        elif "not found in" in exc_msg and "Available catalogs:" in exc_msg:
            err_console.print(
                "\n  Use [bold]--catalog NAME[/bold] with one of the catalogs listed above,\n"
                "  or run [bold]iceberg-meta init[/bold] to add a new one."
            )
        elif "invalid yaml" in exc_msg.lower():
            err_console.print("\n  Fix the syntax error in your config file and try again.")

        raise SystemExit(1) from None

    if not ctx.obj.get("catalog_config") and not CONFIG_FILE.exists() and not catalog_uri:
        err_console.print(
            "[yellow bold]No catalog configured.[/yellow bold]\n\n"
            "  Get started in 30 seconds:\n\n"
            "    [bold]iceberg-meta init[/bold]\n\n"
            "  Or pass a catalog URI directly:\n\n"
            "    [bold]iceberg-meta --uri sqlite:///catalog.db list-tables[/bold]\n"
        )
        raise SystemExit(1)

    ctx.obj["output_format"] = output


# ─── init ─────────────────────────────────────────────────────


_PRESETS: dict[str, dict[str, str]] = {
    "sql": {
        "type": "sql",
        "uri": "${ICEBERG_CATALOG_URI}",
        "warehouse": "${ICEBERG_WAREHOUSE}",
        "s3.endpoint": "${S3_ENDPOINT}",
        "s3.access-key-id": "${AWS_ACCESS_KEY_ID}",
        "s3.secret-access-key": "${AWS_SECRET_ACCESS_KEY}",
        "s3.path-style-access": "true",
        "s3.region": "${AWS_REGION}",
    },
    "glue": {
        "type": "glue",
        "warehouse": "${ICEBERG_WAREHOUSE}",
        "s3.region": "${AWS_REGION}",
    },
    "rest": {
        "type": "rest",
        "uri": "${ICEBERG_REST_URI}",
        "warehouse": "${ICEBERG_WAREHOUSE}",
        "s3.region": "${AWS_REGION}",
    },
    "hive": {
        "type": "hive",
        "uri": "${HIVE_URI}",
        "warehouse": "${ICEBERG_WAREHOUSE}",
        "s3.region": "${AWS_REGION}",
    },
}

_PRESET_DESCRIPTIONS = {
    "sql": "SQL catalog + S3/MinIO  (local dev, CI, SQLite/Postgres)",
    "glue": "AWS Glue Data Catalog  (uses IAM credentials)",
    "rest": "REST catalog  (Tabular, Polaris, or custom)",
    "hive": "Hive Metastore  (Thrift)",
}

_PRESET_ENV_HINTS: dict[str, list[str]] = {
    "sql": [
        "ICEBERG_CATALOG_URI=sqlite:///catalog/iceberg_catalog.db",
        "ICEBERG_WAREHOUSE=s3://warehouse",
        "S3_ENDPOINT=http://localhost:9000",
        "AWS_ACCESS_KEY_ID=admin",
        "AWS_SECRET_ACCESS_KEY=password",
        "AWS_REGION=us-east-1",
    ],
    "glue": [
        "ICEBERG_WAREHOUSE=s3://your-bucket/warehouse",
        "AWS_REGION=us-east-1",
    ],
    "rest": [
        "ICEBERG_REST_URI=https://api.tabular.io/ws",
        "ICEBERG_WAREHOUSE=s3://your-bucket/warehouse",
        "AWS_REGION=us-east-1",
    ],
    "hive": [
        "HIVE_URI=thrift://localhost:9083",
        "ICEBERG_WAREHOUSE=s3://your-bucket/warehouse",
        "AWS_REGION=us-east-1",
    ],
}


@app.command()
def init(ctx: typer.Context) -> None:
    """Interactive setup -- configure a catalog connection.

    Creates or updates ~/.iceberg-meta.yaml. Credentials are stored as
    ${VAR} placeholders so secrets stay in the environment, not on disk.
    """
    console.print("[bold]iceberg-meta setup[/bold]\n")

    if CONFIG_FILE.exists():
        console.print(f"[dim]Config file found: {CONFIG_FILE}[/dim]")
        console.print("[dim]A new catalog will be added alongside existing ones.[/dim]\n")

    # -- preset selection --
    console.print("[bold]Choose your catalog type:[/bold]\n")
    preset_names = list(_PRESETS)
    for i, key in enumerate(preset_names, 1):
        console.print(f"  [bold cyan]{i}[/bold cyan]  {_PRESET_DESCRIPTIONS[key]}")
    console.print()

    choice = typer.prompt("Enter number", default="1")
    try:
        idx = int(choice) - 1
        if idx < 0 or idx >= len(preset_names):
            raise IndexError
        preset_key = preset_names[idx]
    except (ValueError, IndexError):
        err_console.print(f"[red]Invalid choice: {choice}[/red]")
        raise SystemExit(1) from None

    # -- catalog name --
    default_name = preset_key if preset_key != "sql" else "local"
    name = typer.prompt("Catalog name", default=default_name)

    # -- build properties from preset, let user override --
    props = dict(_PRESETS[preset_key])
    console.print("\n[dim]The config uses ${VAR} placeholders resolved from the environment.[/dim]")
    console.print("[dim]Press Enter to keep each default, or type a value to override.[/dim]\n")

    final_props: dict[str, str] = {}
    for key, default_val in props.items():
        label = key
        val = typer.prompt(f"  {label}", default=default_val)
        final_props[key] = val

    make_default = typer.confirm("\nSet as default catalog?", default=True)

    # -- write config --
    try:
        path = merge_config_file(name, final_props, make_default=make_default)
    except ValueError as exc:
        err_console.print(f"[red bold]Config error:[/red bold] {exc}")
        raise SystemExit(1) from None

    console.print(f"\n[green]✓ Saved to {path}[/green]")

    # -- show required env vars --
    placeholders = [v for v in final_props.values() if "${" in str(v)]
    if placeholders:
        console.print("\n[bold]Set these environment variables[/bold] (in .env or your shell):\n")
        env_hints = _PRESET_ENV_HINTS.get(preset_key, [])
        if env_hints:
            for hint in env_hints:
                console.print(f"  {hint}")
        else:
            for v in placeholders:
                var_name = v.replace("${", "").replace("}", "")
                console.print(f"  {var_name}=<your-value>")
        console.print(
            "\n[dim]Tip: place a .env file in your working directory — "
            "iceberg-meta loads it automatically.[/dim]"
        )

    # -- test connection --
    console.print()
    if typer.confirm("Test the connection now?", default=True):
        try:
            load_dotenv(override=True)
            config = resolve_catalog_config(catalog_name=name)
            result = test_connection(config)
            ns = result["namespace_count"]
            tbl = result["table_count"]
            console.print(
                f"\n[green bold]✓ Connected![/green bold]  "
                f"Found {ns} namespace{'s' if ns != 1 else ''}, "
                f"{tbl} table{'s' if tbl != 1 else ''}"
            )
        except Exception as exc:
            console.print(f"\n[yellow]Connection failed:[/yellow] {exc}")
            console.print(
                "[dim]This is normal if the environment variables aren't set yet.\n"
                "Set them and run [bold]iceberg-meta doctor[/bold] to verify.[/dim]"
            )

    console.print(
        "\n[bold]Next steps:[/bold]\n"
        "  [bold]iceberg-meta list-tables[/bold]       Discover tables\n"
        "  [bold]iceberg-meta tui[/bold]               Interactive browser\n"
        "  [bold]iceberg-meta doctor[/bold]             Verify config & connection\n"
    )


# ─── doctor ───────────────────────────────────────────────────


@app.command()
def doctor(ctx: typer.Context) -> None:
    """Check configuration, environment variables, and catalog connectivity."""
    import os
    import re

    load_dotenv(override=True)
    ok_count = 0
    warn_count = 0
    fail_count = 0

    def _ok(msg: str) -> None:
        nonlocal ok_count
        ok_count += 1
        console.print(f"  [green]✓[/green] {msg}")

    def _warn(msg: str) -> None:
        nonlocal warn_count
        warn_count += 1
        console.print(f"  [yellow]![/yellow] {msg}")

    def _fail(msg: str) -> None:
        nonlocal fail_count
        fail_count += 1
        console.print(f"  [red]✗[/red] {msg}")

    console.print("[bold]iceberg-meta doctor[/bold]\n")

    # -- config file --
    console.print("[bold]Config file[/bold]")
    if CONFIG_FILE.exists():
        _ok(f"Found {CONFIG_FILE}")
        try:
            from iceberg_meta.catalog import load_config_file

            file_config = load_config_file()
            catalogs = file_config.get("catalogs", {})
            if catalogs:
                default_cat = file_config.get("default_catalog", "(none)")
                _ok(f"{len(catalogs)} catalog(s) configured: {', '.join(sorted(catalogs))}")
                _ok(f"Default catalog: {default_cat}")
            else:
                _warn("Config file exists but has no catalogs defined")
        except Exception as exc:
            _fail(f"Could not parse config: {exc}")
    else:
        _warn(f"No config file at {CONFIG_FILE} — run [bold]iceberg-meta init[/bold] to create one")
    console.print()

    # -- environment variables --
    console.print("[bold]Environment variables[/bold]")
    dotenv_path = Path(".env")
    if dotenv_path.exists():
        _ok(f".env file found in {dotenv_path.resolve()}")
    else:
        console.print("  [dim]-[/dim] No .env file in current directory (optional)")

    if CONFIG_FILE.exists():
        try:
            file_config = load_config_file()
            all_props = {}
            for cat_props in file_config.get("catalogs", {}).values():
                all_props.update(cat_props)
            referenced_vars: set[str] = set()
            for val in all_props.values():
                if isinstance(val, str):
                    referenced_vars.update(re.findall(r"\$\{(\w+)\}", val))
            if referenced_vars:
                for var in sorted(referenced_vars):
                    env_val = os.environ.get(var)
                    if env_val:
                        is_sensitive = "secret" in var.lower() or "password" in var.lower()
                        display = env_val[:4] + "***" if is_sensitive else env_val
                        _ok(f"${{{var}}} = {display}")
                    else:
                        _fail(f"${{{var}}} is not set")
            else:
                console.print("  [dim]-[/dim] No ${VAR} placeholders used in config")
        except Exception:
            pass
    console.print()

    # -- catalog connectivity --
    console.print("[bold]Catalog connectivity[/bold]")
    try:
        config = resolve_catalog_config()
        _ok(f"Config resolved for catalog '{config.catalog_name}'")
        try:
            result = test_connection(config)
            ns = result["namespace_count"]
            tbl = result["table_count"]
            _ok(
                f"Connected — {ns} namespace{'s' if ns != 1 else ''}, "
                f"{tbl} table{'s' if tbl != 1 else ''}"
            )
        except Exception as exc:
            _fail(f"Connection failed: {exc}")
    except Exception as exc:
        _fail(f"Could not resolve config: {exc}")
    console.print()

    # -- summary --
    total = ok_count + warn_count + fail_count
    if fail_count:
        console.print(
            f"[red bold]{fail_count} problem{'s' if fail_count != 1 else ''}[/red bold] "
            f"found out of {total} checks"
        )
    elif warn_count:
        console.print(
            f"[yellow]All clear[/yellow] with {warn_count} warning{'s' if warn_count != 1 else ''}"
        )
    else:
        console.print("[green bold]Everything looks good![/green bold]")


# ─── demo ─────────────────────────────────────────────────────


@app.command()
def demo(ctx: typer.Context) -> None:
    """Try iceberg-meta with sample data — no config or Docker needed.

    Creates a temporary local catalog with sample tables and launches
    the TUI (or prints a summary if the TUI isn't installed).
    Cleaned up automatically on exit.
    """
    from iceberg_meta.demo import cleanup_demo, create_demo_catalog

    console.print(
        "[bold]iceberg-meta demo[/bold]\n\n  Creating a local catalog with sample tables...\n"
    )

    try:
        demo_dir, props = create_demo_catalog()
    except Exception as exc:
        err_console.print(f"[red bold]Failed to create demo catalog:[/red bold] {exc}")
        raise SystemExit(1) from None

    config = CatalogConfig(catalog_name="demo", properties=props)
    tables_by_ns = list_all_tables(config)
    total = sum(len(ts) for ts in tables_by_ns.values())
    console.print(
        f"  [green]✓[/green] Created {total} tables across {len(tables_by_ns)} namespaces\n"
    )
    for _ns, tables in sorted(tables_by_ns.items()):
        for t in tables:
            tbl = get_table(config, t)
            snaps = len(tbl.metadata.snapshots)
            console.print(f"    {t:<30} {snaps} snapshot{'s' if snaps != 1 else ''}")
    console.print()

    from iceberg_meta.tui.app import IcebergMetaApp

    console.print("  Launching TUI — press [bold]q[/bold] to quit\n")
    try:
        tui_app = IcebergMetaApp(catalog_config=config)
        tui_app.run()
    except Exception as exc:
        err_console.print(f"[red bold]TUI error:[/red bold] {exc}")
    finally:
        cleanup_demo(demo_dir)
        console.print("[dim]Demo data cleaned up.[/dim]")


# ─── quickstart ───────────────────────────────────────────────


_DOCKER_COMPOSE = """\
services:
  minio:
    image: minio/minio:latest
    container_name: iceberg-meta-qs-minio
    environment:
      MINIO_ROOT_USER: admin
      MINIO_ROOT_PASSWORD: password
      MINIO_REGION_NAME: us-east-1
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 5s
      timeout: 3s
      retries: 10
    command: server /data --console-address ":9001"

  init-minio:
    image: minio/mc:latest
    container_name: iceberg-meta-qs-init
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      sh -c "
        mc alias set myminio http://minio:9000 admin password &&
        mc mb --ignore-existing myminio/warehouse &&
        echo 'Bucket ready.'
      "

volumes:
  minio-data:
"""


@app.command()
def quickstart(
    ctx: typer.Context,
    down: bool = typer.Option(False, "--down", help="Stop and remove quickstart containers"),
) -> None:
    """Set up a local playground with Docker (MinIO + sample data).

    Downloads and starts a MinIO container, seeds sample Iceberg tables,
    configures iceberg-meta to connect, and shows you how to explore.
    Requires Docker to be installed and running.

    Use --down to stop and clean up when you're done.
    """
    import subprocess

    if down:
        _quickstart_teardown()
        return

    console.print(
        "[bold]iceberg-meta quickstart[/bold]\n\n"
        "  This will:\n"
        "    1. Start a MinIO container (S3-compatible storage)\n"
        "    2. Create sample Iceberg tables with realistic data\n"
        "    3. Configure iceberg-meta to connect\n"
    )

    # -- check Docker is installed --
    docker_cmd = shutil.which("docker")
    if not docker_cmd:
        err_console.print(
            "[red bold]Docker not found.[/red bold]\n\n"
            "  The quickstart needs Docker to run a local MinIO instance.\n"
            "  Install Docker: [bold]https://docs.docker.com/get-docker/[/bold]\n\n"
            "  [dim]Don't want Docker? Try [bold]iceberg-meta demo[/bold] instead —\n"
            "  it runs entirely in-memory, no containers needed.[/dim]"
        )
        raise SystemExit(1)

    # -- check Docker daemon is running --
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            err_console.print(
                "[red bold]Docker is installed but not running.[/red bold]\n\n"
                "  Start Docker Desktop or the Docker daemon, then try again.\n\n"
                "  [dim]Don't want Docker? Try [bold]iceberg-meta demo[/bold] instead.[/dim]"
            )
            raise SystemExit(1)
    except subprocess.TimeoutExpired:
        err_console.print(
            "[red bold]Docker is not responding.[/red bold]\n\n"
            "  The Docker daemon may be starting up — wait a moment and try again.\n\n"
            "  [dim]Don't want Docker? Try [bold]iceberg-meta demo[/bold] instead.[/dim]"
        )
        raise SystemExit(1) from None

    # -- check docker compose --
    compose_available = False
    try:
        result = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        compose_available = result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    if not compose_available:
        err_console.print(
            "[red bold]Docker Compose not available.[/red bold]\n\n"
            "  The quickstart needs 'docker compose' (v2).\n"
            "  It's included with Docker Desktop, or install it separately:\n"
            "  [bold]https://docs.docker.com/compose/install/[/bold]\n\n"
            "  [dim]Don't want Docker? Try [bold]iceberg-meta demo[/bold] instead.[/dim]"
        )
        raise SystemExit(1)

    # -- check port 9000 --
    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(1)
        port_in_use = sock.connect_ex(("localhost", 9000)) == 0
    finally:
        sock.close()

    if port_in_use:
        err_console.print(
            "[yellow bold]Port 9000 is already in use.[/yellow bold]\n\n"
            "  MinIO needs port 9000. You may already have a quickstart running.\n"
            "  Stop it with: [bold]iceberg-meta quickstart --down[/bold]\n"
            "  Or check what's using the port: [bold]lsof -i :9000[/bold]"
        )
        raise SystemExit(1)

    # -- set up working directory --
    qs_dir = Path.home() / ".iceberg-meta-quickstart"
    qs_dir.mkdir(parents=True, exist_ok=True)
    compose_file = qs_dir / "docker-compose.yml"
    compose_file.write_text(_DOCKER_COMPOSE)

    # -- start containers (stream output so users see pull progress) --
    console.print("  [bold]Starting MinIO...[/bold]\n")
    try:
        proc = subprocess.Popen(
            ["docker", "compose", "up", "-d"],
            cwd=str(qs_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            console.print(f"    [dim]{line.rstrip()}[/dim]")
        rc = proc.wait(timeout=120)
        if rc != 0:
            err_console.print(
                "[red bold]Failed to start containers.[/red bold]\n\n"
                "  Try running manually:\n"
                f"  [bold]cd {qs_dir} && docker compose up -d[/bold]"
            )
            raise SystemExit(1)
    except subprocess.TimeoutExpired:
        proc.kill()
        err_console.print(
            "[red bold]Timed out waiting for containers.[/red bold]\n\n"
            "  Docker may be pulling images (first run can be slow).\n"
            f"  Check status: [bold]cd {qs_dir} && docker compose ps[/bold]"
        )
        raise SystemExit(1) from None

    console.print()

    import time
    import urllib.request

    deadline = time.monotonic() + 30
    healthy = False
    while time.monotonic() < deadline:
        try:
            resp = urllib.request.urlopen("http://localhost:9000/minio/health/live", timeout=2)
            if resp.status == 200:
                healthy = True
                break
        except Exception:
            time.sleep(1)

    if not healthy:
        err_console.print(
            "[red bold]MinIO started but health check failed.[/red bold]\n\n"
            "  Check container status:\n"
            f"  [bold]cd {qs_dir} && docker compose ps[/bold]"
        )
        raise SystemExit(1)

    console.print("  [green]✓[/green] MinIO running on localhost:9000\n")

    # -- seed data --
    console.print("  [bold]Seeding sample tables...[/bold]")

    catalog_dir = qs_dir / "catalog"
    catalog_dir.mkdir(exist_ok=True)

    qs_catalog_props = {
        "type": "sql",
        "uri": f"sqlite:///{catalog_dir / 'iceberg_catalog.db'}",
        "warehouse": "s3://warehouse",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    }

    try:
        from pyiceberg.catalog.sql import SqlCatalog as _SqlCatalog

        from iceberg_meta.demo import _seed_customers, _seed_events, _seed_orders

        cat = _SqlCatalog("quickstart", **qs_catalog_props)

        for ns in ("sales", "analytics"):
            cat.create_namespace_if_not_exists(ns)

        _seed_orders(cat)
        _seed_customers(cat)
        _seed_events(cat)
    except Exception as exc:
        err_console.print(
            f"[red bold]Failed to seed data:[/red bold] {exc}\n\n"
            "  MinIO may still be initializing. Wait a few seconds and try:\n"
            "  [bold]iceberg-meta quickstart[/bold]"
        )
        raise SystemExit(1) from None

    console.print("  [green]✓[/green] Sample tables created\n")

    # -- configure iceberg-meta (use literal values so subsequent commands just work) --
    import contextlib

    with contextlib.suppress(Exception):
        merge_config_file("quickstart", dict(qs_catalog_props), make_default=True)

    # -- show results --
    config = CatalogConfig(catalog_name="quickstart", properties=qs_catalog_props)
    tables_by_ns = list_all_tables(config)
    total = sum(len(ts) for ts in tables_by_ns.values())

    console.print(
        f"  [green bold]Ready![/green bold]  {total} tables across {len(tables_by_ns)} namespaces\n"
    )

    console.print(
        "[bold]Try these:[/bold]\n\n"
        "  iceberg-meta list-tables\n"
        "  iceberg-meta summary sales.orders\n"
        "  iceberg-meta health sales.orders\n"
        "  iceberg-meta schema sales.customers --history\n"
        "  iceberg-meta tree analytics.events\n"
        "  iceberg-meta tui\n\n"
        "[bold]When you're done:[/bold]\n\n"
        "  iceberg-meta quickstart --down\n"
    )


def _quickstart_teardown() -> None:
    """Stop and remove quickstart containers."""
    import contextlib
    import subprocess

    qs_dir = Path.home() / ".iceberg-meta-quickstart"
    compose_file = qs_dir / "docker-compose.yml"

    if not compose_file.exists():
        console.print("[dim]No quickstart environment found.[/dim]")
        return

    console.print("[bold]Stopping quickstart containers...[/bold]")
    with contextlib.suppress(Exception):
        subprocess.run(
            ["docker", "compose", "down", "-v"],
            cwd=str(qs_dir),
            capture_output=True,
            text=True,
            timeout=30,
        )

    shutil.rmtree(qs_dir, ignore_errors=True)

    console.print("[green]✓[/green] Quickstart cleaned up.")


# ─── list-tables ──────────────────────────────────────────────


@app.command("list-tables")
def list_tables_cmd(ctx: typer.Context) -> None:
    """List all namespaces and tables in the catalog."""
    config: CatalogConfig = ctx.obj["catalog_config"]
    fmt: OutputFormat = ctx.obj["output_format"]

    def _do():
        tables_by_ns = list_all_tables(config)
        formatters.render_list_tables(console, tables_by_ns, fmt=fmt)

    _run(_do, config)


# ─── table-info ───────────────────────────────────────────────


@app.command("table-info")
def table_info(
    ctx: typer.Context,
    table: str = typer.Argument(help="Table identifier (namespace.table_name)"),
):
    """Show table overview: format version, location, UUID, schema, partition spec, properties."""
    config: CatalogConfig = ctx.obj["catalog_config"]
    fmt: OutputFormat = ctx.obj["output_format"]

    def _do():
        tbl = get_table(config, table)
        formatters.render_table_info(console, tbl, fmt=fmt)

    _run(_do, config, table)


# ─── snapshots ────────────────────────────────────────────────


@app.command()
def snapshots(
    ctx: typer.Context,
    table: str = typer.Argument(help="Table identifier (namespace.table_name)"),
    watch: int | None = typer.Option(None, "--watch", "-W", help="Refresh every N seconds", min=1),
):
    """List all snapshots with timestamps, operations, and summary stats."""
    config: CatalogConfig = ctx.obj["catalog_config"]
    fmt: OutputFormat = ctx.obj["output_format"]

    if watch is not None:
        _watch_snapshots(config, table, interval=watch, fmt=fmt)
        return

    def _do():
        tbl = get_table(config, table)
        formatters.render_snapshots(console, tbl, fmt=fmt)

    _run(_do, config, table)


def _watch_snapshots(
    config: CatalogConfig, table: str, *, interval: int, fmt: OutputFormat
) -> None:
    """Poll the catalog and refresh snapshots every *interval* seconds."""
    from rich.live import Live
    from rich.table import Table as RichTable

    seen_ids: set[int] = set()
    try:
        with Live(console=console, refresh_per_second=1) as live:
            while True:
                try:
                    tbl = get_table(config, table)
                    current_ids = {s.snapshot_id for s in tbl.metadata.snapshots}
                    new_ids = current_ids - seen_ids
                    seen_ids = current_ids

                    rt = RichTable(
                        title=f"Snapshots (refreshing every {interval}s)", show_lines=True
                    )
                    rt.add_column("Snapshot ID", style="bold")
                    rt.add_column("Timestamp", style="green")
                    rt.add_column("Parent ID")
                    rt.add_column("Operation", style="yellow")
                    rt.add_column("New?", justify="center")

                    for snap in tbl.metadata.snapshots:
                        new = snap.snapshot_id in new_ids
                        marker = "[bold green]NEW[/bold green]" if new else ""
                        rt.add_row(
                            str(snap.snapshot_id),
                            format_timestamp_ms(snap.timestamp_ms),
                            str(snap.parent_snapshot_id or "-"),
                            snap.summary.operation.value if snap.summary else "-",
                            marker,
                        )
                    live.update(rt)
                except Exception as exc:
                    err_console.print(
                        f"[yellow]Watch poll failed:[/yellow] {exc}  (retrying in {interval}s)"
                    )
                time.sleep(interval)
    except KeyboardInterrupt:
        console.print("\n[dim]Watch stopped.[/dim]")


# ─── schema ───────────────────────────────────────────────────


@app.command()
def schema(
    ctx: typer.Context,
    table: str = typer.Argument(help="Table identifier (namespace.table_name)"),
    history: bool = typer.Option(False, "--history", help="Show all historical schemas"),
):
    """Show current schema as a tree. Use --history to see all versions."""
    config: CatalogConfig = ctx.obj["catalog_config"]

    def _do():
        tbl = get_table(config, table)
        if history:
            formatters.render_schema_history(console, tbl)
        else:
            formatters.render_schema(console, tbl.schema())

    _run(_do, config, table)


# ─── manifests ────────────────────────────────────────────────


@app.command()
def manifests(
    ctx: typer.Context,
    table: str = typer.Argument(help="Table identifier (namespace.table_name)"),
    snapshot_id: int | None = typer.Option(
        None, "--snapshot-id", "-s", help="Specific snapshot ID"
    ),
):
    """Show manifest files for the current or specified snapshot."""
    config: CatalogConfig = ctx.obj["catalog_config"]
    fmt: OutputFormat = ctx.obj["output_format"]

    def _do():
        tbl = get_table(config, table)
        formatters.render_manifests(console, tbl, snapshot_id=snapshot_id, fmt=fmt)

    _run(_do, config, table)


# ─── files ────────────────────────────────────────────────────


@app.command()
def files(
    ctx: typer.Context,
    table: str = typer.Argument(help="Table identifier (namespace.table_name)"),
    snapshot_id: int | None = typer.Option(
        None, "--snapshot-id", "-s", help="Specific snapshot ID"
    ),
):
    """Show data files with sizes, row counts, and file format."""
    config: CatalogConfig = ctx.obj["catalog_config"]
    fmt: OutputFormat = ctx.obj["output_format"]

    def _do():
        tbl = get_table(config, table)
        formatters.render_files(console, tbl, snapshot_id=snapshot_id, fmt=fmt)

    _run(_do, config, table)


# ─── partitions ───────────────────────────────────────────────


@app.command()
def partitions(
    ctx: typer.Context,
    table: str = typer.Argument(help="Table identifier (namespace.table_name)"),
):
    """Show partition statistics."""
    config: CatalogConfig = ctx.obj["catalog_config"]
    fmt: OutputFormat = ctx.obj["output_format"]

    def _do():
        tbl = get_table(config, table)
        formatters.render_partitions(console, tbl, fmt=fmt)

    _run(_do, config, table)


# ─── health ───────────────────────────────────────────────────


@app.command()
def health(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Table name (e.g. 'db.table') or Namespace"),
    is_namespace: bool = typer.Option(
        False, "--namespace", "-n", help="Scan all tables in a namespace"
    ),
):
    """Comprehensive table health report.

    File sizes, small-file detection, delete file accumulation,
    partition skew, column null rates, column sizes, and column bounds.
    """
    config: CatalogConfig = ctx.obj["catalog_config"]
    fmt: OutputFormat = ctx.obj["output_format"]

    def _do():
        if is_namespace:
            from pyiceberg.catalog import load_catalog

            catalog = load_catalog(config.catalog_name, **config.properties)
            # Support both "db" and "db.schema" format for namespace
            ns_tuple = tuple(identifier.split("."))

            try:
                tables = catalog.list_tables(ns_tuple)
            except NoSuchNamespaceError:
                # Try listing namespaces to see if it exists but is empty?
                # Or just re-raise and let _friendly_error handle it?
                # _friendly_error handles NoSuchNamespaceError.
                raise

            if not tables:
                console.print(f"[yellow]No tables found in namespace '{identifier}'.[/yellow]")
                return

            count = len(tables)
            console.print(
                f"Found {count} tables in namespace '{identifier}'. Running health checks...\n"
            )

            for i, tbl_tuple in enumerate(tables):
                tbl_name = ".".join(tbl_tuple)
                console.print(f"[bold cyan]Table {i + 1}/{len(tables)}: {tbl_name}[/bold cyan]")
                try:
                    tbl = catalog.load_table(tbl_name)
                    formatters.render_table_health(console, tbl, fmt=fmt)
                    console.print()  # Spacer
                except Exception as e:
                    console.print(f"[red]Skipping {tbl_name}: {e}[/red]\n")
        else:
            tbl = get_table(config, identifier)
            formatters.render_table_health(console, tbl, fmt=fmt)

    _run(_do, config, identifier if not is_namespace else None)


# ─── snapshot-detail ──────────────────────────────────────────


@app.command("snapshot-detail")
def snapshot_detail(
    ctx: typer.Context,
    table: str = typer.Argument(help="Table identifier (namespace.table_name)"),
    snapshot_id: int = typer.Argument(help="Snapshot ID to inspect"),
):
    """Deep dive into a specific snapshot: manifests and files."""
    config: CatalogConfig = ctx.obj["catalog_config"]
    fmt: OutputFormat = ctx.obj["output_format"]

    def _do():
        tbl = get_table(config, table)
        formatters.render_snapshot_detail(console, tbl, snapshot_id, fmt=fmt)

    _run(_do, config, table)


# ─── summary ──────────────────────────────────────────────────


@app.command()
def summary(
    ctx: typer.Context,
    table: str = typer.Argument(help="Table identifier (namespace.table_name)"),
):
    """Show a single-screen summary dashboard with key metrics."""
    config: CatalogConfig = ctx.obj["catalog_config"]
    fmt: OutputFormat = ctx.obj["output_format"]

    def _do():
        tbl = get_table(config, table)
        formatters.render_summary(console, tbl, fmt=fmt)

    _run(_do, config, table)


# ─── diff ─────────────────────────────────────────────────────


@app.command()
def diff(
    ctx: typer.Context,
    table: str = typer.Argument(help="Table identifier (namespace.table_name)"),
    snap1: int = typer.Argument(help="First (older) snapshot ID"),
    snap2: int = typer.Argument(help="Second (newer) snapshot ID"),
):
    """Show what changed between two snapshots."""
    config: CatalogConfig = ctx.obj["catalog_config"]
    fmt: OutputFormat = ctx.obj["output_format"]

    def _do():
        tbl = get_table(config, table)
        formatters.render_diff(console, tbl, snap1, snap2, fmt=fmt)

    _run(_do, config, table)


# ─── tree ─────────────────────────────────────────────────────


@app.command()
def tui(ctx: typer.Context) -> None:
    """Launch the interactive terminal UI."""
    from iceberg_meta.tui.app import IcebergMetaApp

    config: CatalogConfig = ctx.obj["catalog_config"]
    tui_app = IcebergMetaApp(catalog_config=config)
    try:
        tui_app.run()
    except Exception as exc:
        err_console.print(
            f"[red bold]TUI crashed:[/red bold] {exc}\n"
            "  Try the CLI commands instead, e.g. [bold]iceberg-meta list-tables[/bold]"
        )
        raise SystemExit(1) from None


@app.command()
def tree(
    ctx: typer.Context,
    table: str = typer.Argument(help="Table identifier (namespace.table_name)"),
    snapshot_id: int | None = typer.Option(
        None, "--snapshot-id", "-s", help="Specific snapshot (default: current)"
    ),
    max_files: int = typer.Option(
        10, "--max-files", "-m", help="Max data files to show per manifest"
    ),
    all_snapshots: bool = typer.Option(
        False, "--all-snapshots", "-a", help="Show all snapshots in the tree"
    ),
):
    """Visualize the full metadata hierarchy as a tree.

    Snapshot -> Manifest List -> Manifests -> Data Files
    """
    config: CatalogConfig = ctx.obj["catalog_config"]

    def _do():
        tbl = get_table(config, table)
        formatters.render_metadata_tree(
            console,
            tbl,
            snapshot_id=snapshot_id,
            max_files=max_files,
            all_snapshots=all_snapshots,
        )

    _run(_do, config, table)
