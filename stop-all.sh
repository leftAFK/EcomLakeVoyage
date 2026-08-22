#!/bin/bash
# ============================================================
# 停止所有服务
# ============================================================

echo "停止 Hive 服务..."
pkill -f hiveserver2 2>/dev/null || true
pkill -f HiveMetaStore 2>/dev/null || true
echo "Hive 已停止"

echo "停止 Hadoop..."
stop-yarn.sh 2>/dev/null || true
stop-dfs.sh 2>/dev/null || true
echo "Hadoop 已停止"

echo "停止 Docker 容器..."
cd /Users/ok/bigdata/docker
docker compose down
echo "Docker 已停止"

echo ""
echo "全部服务已停止"
