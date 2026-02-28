# Development

Everything needed to develop and test `iceberg-meta` lives in this folder.

## Prerequisites

- Docker & Docker Compose
- [uv](https://docs.astral.sh/uv/) (Python package manager)

## Getting started

```bash
# 1. Install the project in dev mode (from the repo root)
make install

# 2. Create .env from template
make setup          # copies dev/.env.example → .env at project root

# 3. Start MinIO
make infra-up

# 4. Seed sample Iceberg tables
make seed

# 5. Run tests
make test
```

`iceberg-meta` uses [python-dotenv](https://pypi.org/project/python-dotenv/) to auto-load the `.env` file from your working directory -- no `source` or `export` needed.

## Directory layout

```
dev/
├── .env.example              Environment template (credentials, endpoints)
├── docker-compose.yml        MinIO + seed containers
├── docker/
│   ├── minio/                MinIO image + bucket init script
│   └── seed/                 Container that seeds test data on startup
├── scripts/
│   └── seed_local_catalog.py Host-side seeding (used by `make seed`)
├── tests/
│   ├── conftest.py           Shared pytest fixtures
│   ├── data_factory.py       Deterministic test data generator
│   ├── test_cli.py           CLI integration tests
│   └── test_formatters.py    Formatter unit tests
└── DEMO.md                   Step-by-step walkthrough
```

## How credentials flow

No credentials are hard-coded. Every script reads from environment variables with sensible defaults for local development:

| Variable | Default | Used by |
|---|---|---|
| `AWS_ACCESS_KEY_ID` | `admin` | MinIO, seed scripts, tests |
| `AWS_SECRET_ACCESS_KEY` | `password` | MinIO, seed scripts, tests |
| `AWS_REGION` | `us-east-1` | MinIO, seed scripts, tests |
| `S3_ENDPOINT` | `http://localhost:9000` | Seed scripts, tests |
| `ICEBERG_CATALOG_URI` | `sqlite:///catalog/iceberg_catalog.db` | Seed scripts, tests |
| `ICEBERG_WAREHOUSE` | `s3://warehouse` | Seed scripts, tests |

To override any value, edit `.env` at the project root. Changes are picked up on the next run.

## Make targets

Run these from the **repo root**:

```bash
make install      # uv sync --all-extras
make infra-up     # start MinIO (dev/docker-compose.yml)
make seed         # seed local catalog with sample tables
make test         # pytest
make test-cov     # pytest with coverage
make lint         # ruff check
make format       # ruff format
make typecheck    # mypy
make all          # lint + format + typecheck + test
make infra-down   # stop & remove containers
```

## Running the TUI

The interactive terminal UI requires the `textual` package (included in dev dependencies via `make install`):

```bash
uv run iceberg-meta tui
```

This opens a full-screen app with a sidebar table browser and tabbed detail panels. See [DEMO.md](DEMO.md) section 15 for all the key bindings.

## Running tests

Tests require MinIO to be running. The fixtures in `conftest.py` seed the catalog automatically on first use within a session.

```bash
make infra-up
make test
```

To run a single test:

```bash
uv run pytest dev/tests/test_cli.py::test_list_tables -v
```
