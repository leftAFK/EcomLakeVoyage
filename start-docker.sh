#!/bin/bash
# ============================================================
# 启动 Docker 容器 (Kafka / MySQL / Flink / Doris / ClickHouse)
# ============================================================

set -e

echo "=========================================="
echo "  启动 Docker 容器"
echo "=========================================="
cd /Users/ok/bigdata/docker
docker compose up -d

echo ""
echo "容器状态:"
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || docker compose ps
