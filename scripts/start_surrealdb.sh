#!/usr/bin/env bash

# Stop and remove existing container if it exists
docker stop surreallogs-db 2>/dev/null || true
docker rm surreallogs-db 2>/dev/null || true

# Start SurrealDB container
docker run -d \
    --name surreallogs-db \
    -p 8000:8000 \
    surrealdb/surrealdb:v2 \
    start --user root --pass root
