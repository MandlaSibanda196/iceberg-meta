# iceberg-meta Quickstart

Try `iceberg-meta` against a local MinIO warehouse -- the same workflow you would follow after `pip install iceberg-meta` on a real project.

## Prerequisites

- Docker & Docker Compose
- Python 3.10+

## 1. Set up environment

```bash
cp .env.example .env        # edit if you need different credentials
```

`iceberg-meta` auto-loads `.env` from your working directory -- no `source` or `export` needed.

## 2. Start MinIO

```bash
docker compose up -d
```

MinIO console is available at http://localhost:9001 (credentials from your `.env`).

## 3. Install iceberg-meta

From PyPI (when published):

```bash
pip install iceberg-meta
```

Or install from the local repo (for development):

```bash
pip install -e ..
```

## 4. Configure

Copy the config template and source your environment:

```bash
cp iceberg-meta.yaml ~/.iceberg-meta.yaml
```

The config uses `${VAR}` placeholders that resolve from your `.env` at runtime -- no manual sourcing needed.

## 5. Seed sample data

```bash
python seed.py
```

You should see:

```
Seeding sample tables ...

  sales.orders          4 snapshot(s)
  analytics.events      3 snapshot(s)
  staging.metrics       1 snapshot(s)

Done. Try:
  iceberg-meta list-tables
  iceberg-meta summary sales.orders
  iceberg-meta tree analytics.events
```

## 6. Verify

```bash
iceberg-meta doctor
```

You should see all green checks. If any variables are missing, double-check your `.env` file.

## 7. Explore

```bash
# Discover tables
iceberg-meta list-tables

# Quick health check
iceberg-meta summary sales.orders

# Full metadata tree
iceberg-meta tree sales.orders

# Snapshot history
iceberg-meta snapshots analytics.events

# Compare two snapshots (grab IDs from the snapshots command)
iceberg-meta diff analytics.events <snap-id-1> <snap-id-2>

# JSON output for scripting
iceberg-meta -o json summary sales.orders | jq '.'

# CSV export
iceberg-meta -o csv files sales.orders > files.csv
```

## 8. Interactive TUI (optional)

For a visual, interactive experience instead of individual commands:

```bash
iceberg-meta tui
```

This opens a full-screen terminal app where you can browse tables in a sidebar, view summary, snapshots, schema, files, and tree in tabbed panels -- no commands to memorize.

## 9. Teardown

```bash
docker compose down -v
```

## Using with your own catalog

Copy the example config from the repo and edit it for your catalog:

```bash
cp ../examples/iceberg-meta.yaml ~/.iceberg-meta.yaml
```

Set your credentials in a `.env` file or as environment variables (never hard-code them in the config):

```bash
# .env file (iceberg-meta loads this automatically)
ICEBERG_CATALOG_URI=your-catalog-uri
ICEBERG_WAREHOUSE=s3://your-bucket/warehouse
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
AWS_REGION=us-east-1
```

See [examples/iceberg-meta.yaml](../examples/iceberg-meta.yaml) for Glue, REST, Nessie, Hive, and Hadoop catalog templates.
