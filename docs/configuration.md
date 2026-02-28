# Configuration

## Config file

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

See [examples/iceberg-meta.yaml](../examples/iceberg-meta.yaml) for configs covering Glue, REST, Nessie, Hive, and Hadoop catalogs.

## Interactive setup

```bash
iceberg-meta init
```

Walks you through catalog type selection, writes `~/.iceberg-meta.yaml` with `${VAR}` placeholders, and tests the connection.

## Environment variable overrides

These override any config file value without needing `${VAR}` syntax:

| Variable | Maps to |
|---|---|
| `ICEBERG_META_CATALOG_URI` | `uri` |
| `ICEBERG_META_WAREHOUSE` | `warehouse` |
| `ICEBERG_META_S3_ENDPOINT` | `s3.endpoint` |
| `ICEBERG_META_S3_ACCESS_KEY` | `s3.access-key-id` |
| `ICEBERG_META_S3_SECRET_KEY` | `s3.secret-access-key` |
| `ICEBERG_META_S3_REGION` | `s3.region` |

## .env file support

`iceberg-meta` uses [python-dotenv](https://pypi.org/project/python-dotenv/) to auto-load a `.env` file from your working directory:

- **No `source` or `export` needed** -- just place a standard `.env` file in your project
- **Use any variable names you already have** -- reference them in your config with `${MY_VAR}`
- **Point to a specific file** if your `.env` is elsewhere: `iceberg-meta --env-file path/to/.env`
- **Already-exported shell variables take precedence** over `.env` values (standard dotenv behavior)

Data engineers who already have AWS credentials or catalog URIs in their environment don't need a `.env` file at all -- the `${VAR}` placeholders in the config resolve against whatever is already set.

## Global CLI options

| Option | Description |
|---|---|
| `--catalog, -c` | Catalog name (as defined in config) |
| `--uri` | Catalog URI override |
| `--warehouse, -w` | Warehouse path override |
| `--output, -o` | Output format: `table` (default), `json`, `csv` |
| `--env-file, -e` | Path to `.env` file (auto-loads `.env` in cwd by default) |
