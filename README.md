# iceberg-meta

CLI and TUI for exploring Apache Iceberg table metadata. Inspect snapshots, schemas, manifests, data files, partition health, and column-level statistics -- without Spark or notebooks.

## Why use this?

While the official `pyiceberg` CLI is great for basic operations, `iceberg-meta` is built specifically for **data engineers** who need to debug, optimize, and understand their tables.

*   **⚡ Instant Feedback:** No Spark cluster startup time.
*   **🔍 Deep Visibility:** Inspect manifest lists, manifest files, and individual data files.
*   **🏥 Health Checks:** Detect small files, partition skew, and compaction needs in seconds.
*   **📉 Diff Everything:** Compare two snapshots to see exactly what changed (files added/deleted, rows changed).
*   **🖥️ Interactive TUI:** Browse your catalog with a powerful terminal UI.
*   **🤖 CI/CD Ready:** Output JSON/CSV for automated checks in your pipelines.

## Performance & Benchmarks

We take performance seriously. Data engineers shouldn't wait minutes for simple metadata checks.

**Benchmark Scenario:**
- **Table:** 5,000 data files, 50,000 rows (simulating a "small file problem" scenario).
- **Environment:** Local development machine (SQLite catalog).
- **Task:** Full metadata scan, partition skew calculation, and file listing.

| Command | Task | Time | vs Spark (Local) |
|---|---|---|---|
| `summary` | Read latest snapshot & schema | **~1.5s** | ~15s+ (startup) |
| `health` | Full scan of 5,000 files (skew, nulls, bounds) | **~1.5s** | ~20s+ |
| `files` | List all 5,000 file paths & stats | **~2.1s** | ~20s+ |
| `tree` | Render TUI with lazy loading | **Instant** | N/A |

*Note: Spark times include JVM startup overhead, which is the main friction point `iceberg-meta` eliminates for quick checks.*

## Install

### From PyPI

```bash
pip install iceberg-meta
```

### From Homebrew (macOS/Linux)

You can install directly from the repository:

```bash
brew tap MandlaSibanda196/iceberg-meta https://github.com/MandlaSibanda196/iceberg-meta
brew install iceberg-meta
```

## Try it instantly

No config, no Docker, no credentials:

```bash
iceberg-meta demo
```

Creates a temporary catalog with sample tables, launches the TUI, and cleans up on exit.

## Quick Start

**Connect to your data:**

```bash
iceberg-meta init              # interactive config setup
iceberg-meta doctor            # verify config + connectivity
iceberg-meta list-tables       # explore
iceberg-meta tui               # interactive browser
```

**Or spin up a Docker playground:**

```bash
iceberg-meta quickstart        # starts MinIO, seeds data, configures everything
iceberg-meta quickstart --down # tear down when done
```

## Commands

| Command | Description |
|---|---|
| `demo` | Try instantly -- temp local catalog, no setup needed |
| `quickstart` | Docker playground with MinIO + sample data |
| `init` | Interactive config setup with catalog presets |
| `doctor` | Validate config, env vars, and connectivity |
| `list-tables` | Discover namespaces and tables |
| `summary <table>` | Row counts, file counts, recent operations |
| `health <table>` | File sizes, partition skew, column nulls, column bounds |
| `table-info <table>` | Format version, UUID, schema, partition spec, properties |
| `snapshots <table>` | Snapshot history (`--watch N` for live monitoring) |
| `schema <table>` | Schema tree (`--history` for evolution with diffs) |
| `manifests <table>` | Manifest files for current or specified snapshot |
| `files <table>` | Data files with sizes, row counts, format |
| `partitions <table>` | Partition statistics |
| `snapshot-detail <table> <id>` | Deep dive into one snapshot |
| `diff <table> <s1> <s2>` | What changed between two snapshots |
| `tree <table>` | Full metadata hierarchy as a tree |
| `tui` | Interactive terminal UI |

Every data command supports `--output json` and `--output csv`.

## Use Cases

### Pre-merge validation in CI/CD

Confirm a pipeline write actually landed before merging:

```bash
iceberg-meta summary staging.orders          # row count, file count, latest snapshot
iceberg-meta diff staging.orders $OLD $NEW   # what changed between two snapshots
iceberg-meta files staging.orders -o csv     # pipe file-level stats into a check script
```

### Debugging failing writes

Spark job "succeeded" but downstream dashboards are empty:

```bash
iceberg-meta snapshots staging.orders        # did the snapshot actually land?
iceberg-meta schema staging.orders --history # did a schema evolution break compatibility?
iceberg-meta files staging.orders            # are the new data files present and non-empty?
```

### Monitoring table health

Spot small-file problems, partition skew, and compaction needs before they impact query performance:

```bash
iceberg-meta health warehouse.events
```

The health report covers file sizes (min/avg/median/max with small-file warnings), delete file accumulation, partition skew detection, column null rates, column storage distribution, and column value bounds.

### Live monitoring

Watch for new snapshots as a pipeline runs:

```bash
iceberg-meta snapshots warehouse.events --watch 5
```

### Onboarding and knowledge transfer

New team member needs to understand the data platform:

```bash
iceberg-meta tui
```

### Incident response

Production data looks wrong — find when the issue was introduced:

```bash
iceberg-meta snapshots prod.customers        # find the suspicious snapshot IDs
iceberg-meta diff prod.customers 111 222     # compare record counts and file changes
iceberg-meta tree prod.customers             # drill into manifests and data files
```

### Scripting and automation

Pipe machine-readable output into alerts or dashboards:

```bash
FILE_COUNT=$(iceberg-meta -o json summary db.events | jq '.file_count')
[ "$FILE_COUNT" -gt 1000 ] && echo "Small file problem detected"

iceberg-meta -o csv snapshots db.events > snapshots.csv
iceberg-meta -o json health db.events | jq '.[] | select(.Section == "Column Nulls")'
```

## See it in action

```
$ iceberg-meta summary sales.orders

┌─────────────────────────────────────────────────────────────────┐
│                     sales.orders  Summary                       │
├──────────────────────┬──────────────────────────────────────────┤
│ Format version       │ 2                                        │
│ Total snapshots      │ 4                                        │
│ Total data files     │ 1                                        │
│ Total records        │ 15                                       │
│ Total size           │ 5.2 KB                                   │
│ Partition spec       │ region (identity)                        │
├──────────────────────┴──────────────────────────────────────────┤
│ Recent Operations                                               │
│  overwrite   2025-02-28 19:04    +15 rows   -60 rows            │
│  append      2025-02-28 19:04    +20 rows   -0 rows             │
│  append      2025-02-28 19:04    +15 rows   -0 rows             │
└─────────────────────────────────────────────────────────────────┘
```

```
$ iceberg-meta schema sales.customers --history

Schema 0  (initial)
├── customer_id: long
├── name: string
└── email: string

Schema 1  (+2 fields)
├── phone: string          ← added
└── signup_date: date      ← added

Schema 2  (1 rename)
└── email_address: string  ← renamed from email
```

## TUI Keybindings

| Key | Action |
|---|---|
| `1`-`7` | Switch tabs (Summary, Snapshots, Schema, Files, Manifests, Health, Tree) |
| `d` | Diff two snapshots |
| `s` | Snapshot detail |
| `r` | Refresh |
| `?` | Help |
| `q` | Quit |

## Configuration

Run `iceberg-meta init` for interactive setup, or create `~/.iceberg-meta.yaml` manually. Credentials use `${VAR}` placeholders resolved from the environment -- secrets never touch disk.

See [docs/configuration.md](docs/configuration.md) for full details, environment variable overrides, and `.env` file support.

## License

[MIT](LICENSE)
