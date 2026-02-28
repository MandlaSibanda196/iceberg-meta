# iceberg-meta

CLI and TUI for exploring Apache Iceberg table metadata. Lightweight, terminal-native, and scriptable -- inspect snapshots, schemas, manifests, data files, partition health, and column-level statistics without spinning up a Spark shell or writing a notebook.

## Why iceberg-meta?

Iceberg tables store rich metadata -- schema evolution history, snapshot lineage, manifest-level statistics, column bounds, and more. But accessing any of it usually means writing PySpark code or digging through Avro files by hand.

`iceberg-meta` gives you instant access to all of it from the terminal:

- **One command to see everything:** `iceberg-meta health sales.orders` shows file sizes, small-file warnings, partition skew, column null rates, column storage distribution, and value bounds -- all at once.
- **Interactive exploration:** the TUI lets you browse tables, schemas, and snapshots visually without memorizing flags.
- **Scriptable output:** every command supports `--output json` and `--output csv` for CI/CD pipelines and alerting.
- **Zero infrastructure:** works with any catalog pyiceberg supports (SQL, REST, Glue, Hive, Nessie, Hadoop).

## Install

```bash
pip install iceberg-meta

# With the interactive TUI (optional)
pip install iceberg-meta[tui]
```

## Quick Start

```bash
# 1. Configure — picks your catalog type, writes ~/.iceberg-meta.yaml
#    with ${VAR} placeholders (secrets stay in the environment)
iceberg-meta init

# 2. Verify — checks config, env vars, and catalog connectivity
iceberg-meta doctor

# 3. Explore
iceberg-meta list-tables
iceberg-meta summary sales.orders
iceberg-meta health sales.orders
iceberg-meta tree sales.orders
```

Or launch the interactive TUI to browse everything visually:

```bash
iceberg-meta tui
```

See the [quickstart/](quickstart/) folder for a guided walkthrough with Docker.

## Commands

| Command | Description |
|---|---|
| `init` | Interactive config setup -- catalog presets, `${VAR}` placeholders, connection test |
| `doctor` | Validate config file, environment variables, and catalog connectivity |
| `list-tables` | Discover namespaces and tables |
| `summary <table>` | Single-screen dashboard: row counts, file counts, recent operations |
| `health <table>` | Comprehensive health report: file sizes, small-file detection, partition skew, column null rates, column sizes, column bounds |
| `table-info <table>` | Format version, UUID, location, schema, partition spec, properties |
| `snapshots <table>` | All snapshots with timestamps, operations, summary (`--watch N`) |
| `schema <table>` | Current schema as a tree (`--history` for all versions with diffs) |
| `manifests <table>` | Manifest files for current or specified snapshot |
| `files <table>` | Data files with sizes, row counts, format |
| `partitions <table>` | Partition statistics |
| `snapshot-detail <table> <id>` | Deep dive into one snapshot: manifests + files |
| `diff <table> <snap1> <snap2>` | What changed between two snapshots |
| `tree <table>` | Full metadata hierarchy as a tree (`--all-snapshots`) |
| `tui` | Interactive terminal UI -- browse tables and metadata visually |

## Use Cases

### Pre-merge validation in CI/CD

Before merging a pipeline PR, confirm the write actually produced the expected outcome:

```bash
iceberg-meta summary staging.orders          # row count, file count, latest snapshot
iceberg-meta diff staging.orders $OLD $NEW   # what changed between two snapshots
iceberg-meta files staging.orders -o csv     # pipe file-level stats into a check script
```

### Debugging failing writes

A Spark job "succeeded" but downstream dashboards are empty. Quickly narrow the problem:

```bash
iceberg-meta snapshots staging.orders        # did the snapshot actually land?
iceberg-meta schema staging.orders --history # did a schema evolution break compatibility?
iceberg-meta files staging.orders            # are the new data files present and non-empty?
```

### Monitoring table health

Spot small-file problems, partition skew, and compaction needs before they impact query performance:

```bash
iceberg-meta health warehouse.events         # full health report in one command
```

The health report includes:
- **File health:** min/avg/median/max sizes, small-file warnings (< 32 MB)
- **Delete files:** data vs delete manifest counts, compaction recommendations
- **Partition skew:** per-partition file counts and row counts with skew detection
- **Column null rates:** percentage of nulls per column, color-coded by severity
- **Column sizes:** storage distribution with bar charts showing which columns are largest
- **Column bounds:** min/max values per column from file-level statistics

### Live monitoring

Watch for new snapshots as a pipeline runs:

```bash
iceberg-meta snapshots warehouse.events --watch 5  # refresh every 5 seconds
```

### Onboarding and knowledge transfer

A new team member needs to understand the data platform. The TUI lets them browse interactively without memorizing commands:

```bash
iceberg-meta tui
```

### Incident response

Production data looks wrong. Compare snapshots to find when the issue was introduced:

```bash
iceberg-meta snapshots prod.customers        # find the suspicious snapshot IDs
iceberg-meta diff prod.customers 111 222     # compare record counts and file changes
iceberg-meta tree prod.customers             # drill into manifests and data files
```

### Scripting and automation

Pipe machine-readable output into other tools:

```bash
# Alert if file count exceeds threshold
FILE_COUNT=$(iceberg-meta -o json summary db.events | jq '.file_count')
[ "$FILE_COUNT" -gt 1000 ] && echo "Small file problem detected"

# Export snapshot history to CSV for a report
iceberg-meta -o csv snapshots db.events > snapshots.csv

# Health data as JSON for a monitoring dashboard
iceberg-meta -o json health db.events | jq '.[] | select(.Section == "Column Nulls")'
```

## TUI

The interactive TUI (`iceberg-meta tui`) covers nearly all CLI functionality in a single screen. Press `?` inside the TUI for a full keybinding reference.

| Key | Tab / Action |
|---|---|
| `1` | **Summary** -- table overview, recent operations, file health indicators |
| `2` | **Snapshots** -- snapshot history with operations and summary |
| `3` | **Schema** -- schema evolution with diffs between versions |
| `4` | **Files** -- data files with size distribution stats (min/avg/median/max) |
| `5` | **Manifests** -- manifest files for current snapshot |
| `6` | **Health** -- file sizes, partition skew, column nulls, column sizes, column bounds |
| `7` | **Tree** -- full metadata hierarchy (snapshot > manifest list > manifests > files) |
| `d` | **Diff** -- compare two snapshots (modal) |
| `s` | **Detail** -- snapshot deep-dive (modal) |
| `r` | Refresh all panels |
| `?` | Help screen with all keybindings and CLI equivalents |
| `q` | Quit |

The sidebar always shows the namespace/table tree (equivalent to `list-tables`).

**CLI-only features:** `init` (interactive config setup), `snapshots --watch N` (live-watch mode), `table-info` (UUID, properties, partition spec), `partitions` (basic table view), `--output json|csv` (machine-readable output).

## Configuration

### Config file with ${VAR} placeholders

Create `~/.iceberg-meta.yaml`. Values wrapped in `${VAR}` are resolved from the environment at runtime -- never hard-code credentials:

```yaml
default_catalog: production

catalogs:
  production:
    type: glue
    warehouse: ${ICEBERG_WAREHOUSE}
    s3.region: ${AWS_REGION}

  staging:
    type: sql
    uri: ${ICEBERG_CATALOG_URI}
    warehouse: ${ICEBERG_WAREHOUSE}
    s3.endpoint: ${S3_ENDPOINT}
    s3.access-key-id: ${AWS_ACCESS_KEY_ID}
    s3.secret-access-key: ${AWS_SECRET_ACCESS_KEY}
    s3.region: ${AWS_REGION}
```

See [examples/iceberg-meta.yaml](examples/iceberg-meta.yaml) for configs covering Glue, REST, Nessie, Hive, and Hadoop catalogs.

### Environment variable overrides

These override any config file value without needing `${VAR}` syntax:

| Variable | Maps to |
|---|---|
| `ICEBERG_META_CATALOG_URI` | `uri` |
| `ICEBERG_META_WAREHOUSE` | `warehouse` |
| `ICEBERG_META_S3_ENDPOINT` | `s3.endpoint` |
| `ICEBERG_META_S3_ACCESS_KEY` | `s3.access-key-id` |
| `ICEBERG_META_S3_SECRET_KEY` | `s3.secret-access-key` |
| `ICEBERG_META_S3_REGION` | `s3.region` |

### Interactive setup

```bash
iceberg-meta init
```

### Environment variables

`iceberg-meta` uses [python-dotenv](https://pypi.org/project/python-dotenv/) to auto-load a `.env` file from your working directory:

- **No `source` or `export` needed** -- just place a standard `.env` file in your project
- **Use any variable names you already have** -- reference them in your config with `${MY_VAR}`
- **Point to a specific file** if your `.env` is elsewhere: `iceberg-meta --env-file path/to/.env`
- **Already-exported shell variables take precedence** over `.env` values (standard dotenv behavior)

Data engineers who already have AWS credentials or catalog URIs in their environment don't need a `.env` file at all -- the `${VAR}` placeholders in the config resolve against whatever is already set.

## Global Options

| Option | Description |
|---|---|
| `--catalog, -c` | Catalog name (as defined in config) |
| `--uri` | Catalog URI override |
| `--warehouse, -w` | Warehouse path override |
| `--output, -o` | Output format: `table` (default), `json`, `csv` |
| `--env-file, -e` | Path to `.env` file (auto-loads `.env` in cwd by default) |

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    iceberg-meta CLI                      │
│                      (Typer app)                         │
├──────────────┬──────────────────────┬───────────────────┤
│  catalog.py  │    formatters.py     │     utils.py      │
│              │                      │                   │
│  Config      │  Rich Tables/Trees   │  format_bytes()   │
│  resolution  │  for each command    │  format_time()    │
│  + ${VAR}    │                      │  truncate_path()  │
├──────────────┴──────────────────────┴───────────────────┤
│                    pyiceberg                             │
│          (catalog, table, inspect APIs)                  │
├─────────────────────────────────────────────────────────┤
│         Any Iceberg Catalog (SQL, REST, Glue, Hive)     │
├─────────────────────────────────────────────────────────┤
│              S3 / MinIO / HDFS / Local Storage           │
│          (Parquet data + Avro metadata)                  │
└─────────────────────────────────────────────────────────┘
```

## Project Layout

```
iceberg-meta/
│
├── src/iceberg_meta/      Package source
│   ├── catalog.py         Config resolution + ${VAR} expansion
│   ├── cli.py             Typer commands
│   ├── formatters.py      Rich table / tree renderers + health analysis
│   ├── output.py          JSON, CSV, Rich table output
│   ├── utils.py           Byte / timestamp formatting helpers
│   └── tui/               Interactive terminal UI (optional)
│
├── quickstart/            Try it locally with Docker
│   ├── .env.example       Credentials template
│   ├── docker-compose.yml MinIO (lightweight)
│   ├── iceberg-meta.yaml  Config with ${VAR} placeholders
│   ├── seed.py            Sample data creator
│   └── README.md          Getting-started guide
│
├── examples/              Sample configs for real catalogs
│   └── iceberg-meta.yaml  Glue, REST, Nessie, Hive, Hadoop templates
│
├── pyproject.toml         Package definition
└── LICENSE                MIT license
```

## Development

Requires [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
uv sync --all-extras        # install all dependencies
uv run ruff check src/      # lint
uv run ruff format src/     # format
uv run mypy src/            # type check
uv build                    # build sdist + wheel
```
