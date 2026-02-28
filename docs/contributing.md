# Contributing

## Setup

Requires [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
uv sync --all-extras        # install all dependencies
uv run ruff check src/      # lint
uv run ruff format src/     # format
uv run mypy src/            # type check
uv build                    # build sdist + wheel
```

## Project layout

```
iceberg-meta/
├── src/iceberg_meta/      Package source
│   ├── catalog.py         Config resolution + ${VAR} expansion
│   ├── cli.py             Typer commands
│   ├── demo.py            Demo catalog for instant onboarding
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
├── docs/                  Documentation
├── pyproject.toml         Package definition
└── LICENSE                MIT license
```

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

## CI/CD

CI runs on every push to `main` and on tags:

- **Lint**: ruff check + ruff format + mypy
- **Publish**: triggered by `v*` tags, publishes to PyPI via trusted publishing

To release a new version:

```bash
# Update version in src/iceberg_meta/__init__.py
# Commit and tag
git tag v0.x.x
git push origin v0.x.x
```
