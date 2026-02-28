# Iceberg Metadata Explorer -- Dev Walkthrough

Step-by-step guide to spin up infrastructure, seed test data, configure the CLI, and exercise every command. All `make` commands run from the repo root.

## 1. Start Infrastructure

```bash
cd iceberg-meta
make setup          # creates .env from template (first time only)
make infra-up
```

Wait for the seed container to finish (~30 seconds):

```bash
docker compose -f dev/docker-compose.yml logs seed --follow
```

You should see:

```
seed  |   analytics.events               5 snapshot(s)
seed  |   analytics.sessions             3 snapshot(s)
seed  |   sales.customers                3 snapshot(s)
seed  |   sales.orders                   5 snapshot(s)
seed  |   staging.landing_raw            0 snapshot(s)
seed  |   staging.wide_metrics           1 snapshot(s)
seed  |
seed  | Done -- seeded 6 tables.
```

## 2. Install the CLI

```bash
make install
```

Verify it works:

```bash
uv run iceberg-meta --help
```

## 3. Configure

### Option A -- Copy the quickstart config

```bash
cp quickstart/iceberg-meta.yaml ~/.iceberg-meta.yaml
```

The config uses `${VAR}` placeholders that resolve from your environment at runtime. `iceberg-meta` auto-loads `.env` from your working directory -- no manual `source` needed.

### Option B -- Interactive setup

```bash
uv run iceberg-meta init
```

When prompted, use the values from `.env` (or `dev/.env.example`):

| Prompt | Value |
|---|---|
| Catalog name | `local` |
| Catalog type | `sql` |
| Catalog URI | value of `$ICEBERG_CATALOG_URI` |
| Warehouse path | value of `$ICEBERG_WAREHOUSE` |
| S3 endpoint | value of `$S3_ENDPOINT` |
| S3 access key | value of `$AWS_ACCESS_KEY_ID` |
| S3 secret key | value of `$AWS_SECRET_ACCESS_KEY` |
| S3 region | value of `$AWS_REGION` |
| Set as default? | `Y` |

## 4. Discover Tables

See what namespaces and tables exist in the catalog:

```bash
uv run iceberg-meta list-tables
```

JSON output for scripting:

```bash
uv run iceberg-meta -o json list-tables
```

## 5. Summary Dashboard

Quick health-check overview -- snapshot count, data files, total size, recent operations:

```bash
uv run iceberg-meta summary sales.orders
```

## 6. Table Info

Full overview: format version, UUID, location, schema, partition spec, and properties:

```bash
uv run iceberg-meta table-info sales.orders
```

`sales.orders` has custom properties (owner, write format, data classification) and is partitioned by `region`.

## 7. Schema

Current schema as a tree showing field names, types, required/optional, and IDs:

```bash
uv run iceberg-meta schema sales.orders
```

With `--history` to see every schema version. `sales.customers` has 3 schema versions (added columns + rename):

```bash
uv run iceberg-meta schema sales.customers --history
```

For nested struct types, try `analytics.events` which has a `device` struct:

```bash
uv run iceberg-meta schema analytics.events
```

For a wide table (22 columns), try:

```bash
uv run iceberg-meta schema staging.wide_metrics
```

## 8. Snapshots

List all snapshots with timestamps, operations, and summary stats:

```bash
uv run iceberg-meta snapshots sales.orders
```

`sales.orders` has 5 snapshots (3 appends + 1 spec update + 1 overwrite).

`analytics.events` has 5 pure append snapshots:

```bash
uv run iceberg-meta snapshots analytics.events
```

Get the output as JSON for piping into `jq`:

```bash
uv run iceberg-meta -o json snapshots sales.orders | jq '.[0]."Snapshot ID"'
```

### Watch mode

Live-refresh every 5 seconds (press Ctrl+C to stop). New snapshots are highlighted:

```bash
uv run iceberg-meta snapshots sales.orders --watch 5
```

## 9. Manifests

Show manifest files for the current snapshot:

```bash
uv run iceberg-meta manifests sales.orders
```

For a specific snapshot (use a snapshot ID from step 8):

```bash
uv run iceberg-meta manifests sales.orders --snapshot-id <SNAPSHOT_ID>
```

## 10. Files

Show data files with row counts, sizes, and file format:

```bash
uv run iceberg-meta files sales.orders
```

Export to CSV:

```bash
uv run iceberg-meta -o csv files sales.orders > files.csv
cat files.csv
```

## 11. Partitions

Partition statistics -- record counts, file counts, total size per partition.

`sales.orders` is partitioned by region:

```bash
uv run iceberg-meta partitions sales.orders
```

## 12. Health Report

Comprehensive table health analysis -- file sizes, small-file detection, delete file accumulation, partition skew, column null rates, column sizes, and column bounds:

```bash
uv run iceberg-meta health sales.orders
```

For JSON output (useful for monitoring dashboards):

```bash
uv run iceberg-meta -o json health sales.orders
```

## 13. Snapshot Detail

Deep dive into a single snapshot -- combines snapshot header, manifests, and files.

First grab two snapshot IDs from step 8, then inspect the latest one:

```bash
uv run iceberg-meta snapshot-detail sales.orders <SNAPSHOT_ID>
```

## 14. Diff Between Snapshots

Compare two snapshots to see what files were added or removed.

Grab two snapshot IDs from step 8 (an older and a newer one):

```bash
uv run iceberg-meta diff sales.orders <OLDER_SNAPSHOT_ID> <NEWER_SNAPSHOT_ID>
```

JSON output:

```bash
uv run iceberg-meta -o json diff sales.orders <OLDER_SNAPSHOT_ID> <NEWER_SNAPSHOT_ID>
```

## 15. Tree (Signature Feature)

Visualize the full metadata hierarchy: Snapshot -> Manifest List -> Manifests -> Data Files.

File sizes are color-coded (green = small, yellow = medium, red = large). Each manifest shows the percentage of total rows it holds.

```bash
uv run iceberg-meta tree sales.orders
```

Limit data files shown per manifest:

```bash
uv run iceberg-meta tree sales.orders --max-files 3
```

Show every snapshot in one tree:

```bash
uv run iceberg-meta tree sales.orders --all-snapshots
```

A specific snapshot only:

```bash
uv run iceberg-meta tree sales.orders --snapshot-id <SNAPSHOT_ID>
```

## 16. Interactive TUI

Launch the full interactive terminal UI -- browse tables, switch tabs, explore metadata without typing commands:

```bash
uv run iceberg-meta tui
```

You get a sidebar with all your catalog tables, and tabbed panels on the right for Summary, Snapshots, Schema, Files, Manifests, Health, and Tree views.

Key bindings inside the TUI:

| Key | Action |
|---|---|
| Arrow keys / click | Navigate sidebar |
| `1`-`7` | Switch tabs (Summary, Snapshots, Schema, Files, Manifests, Health, Tree) |
| `d` | Diff two snapshots (from Snapshots tab) |
| `s` | Snapshot detail (deep-dive on selected snapshot) |
| `r` | Refresh data from catalog |
| `?` | Help screen with all keybindings and CLI equivalents |
| `q` | Quit |

Use a specific catalog:

```bash
uv run iceberg-meta --catalog production tui
```

## 17. Empty Table and Edge Cases

`staging.landing_raw` has no snapshots (empty table):

```bash
uv run iceberg-meta tree staging.landing_raw
uv run iceberg-meta snapshots staging.landing_raw
uv run iceberg-meta schema staging.landing_raw
```

## 18. Output Formats

Every command above supports `--output` / `-o` with three modes:

| Flag | Description |
|---|---|
| `-o table` | Rich formatted table (default) |
| `-o json` | JSON array, pipe to `jq` |
| `-o csv` | CSV, redirect to a file |

Examples:

```bash
uv run iceberg-meta -o json summary sales.orders
uv run iceberg-meta -o csv partitions sales.orders > partitions.csv
uv run iceberg-meta -o json files analytics.events | jq '.[].["File Path"]'
```

## 19. Run Tests

Make sure infrastructure is running, then (from repo root):

```bash
make test
```

With coverage:

```bash
make test-cov
```

## 20. Lint and Typecheck

```bash
make lint
make typecheck
```

Auto-format:

```bash
make format
```

Run everything:

```bash
make all
```

## 21. Teardown

```bash
make infra-down
```
