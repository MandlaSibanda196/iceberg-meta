FROM minio/minio:latest

EXPOSE 9000 9001

HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
    CMD curl -f http://localhost:9000/minio/health/live || exit 1

CMD ["server", "/data", "--console-address", ":9001"]
