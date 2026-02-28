#!/bin/sh
# Wait for MinIO to be ready, then create the warehouse bucket.
# MINIO_ROOT_USER and MINIO_ROOT_PASSWORD are injected by docker-compose.
set -e

until mc alias set myminio http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"; do
    echo "Waiting for MinIO..."
    sleep 2
done

mc mb --ignore-existing myminio/warehouse
mc anonymous set public myminio/warehouse

echo "MinIO bucket 'warehouse' created."
