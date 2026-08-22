#!/bin/bash
# ============================================================
# Grafana + Prometheus 一键部署脚本
# 在 Mac 终端执行：bash setup-monitoring.sh
#
# 功能：
#   1. 自动检测现有 Docker 网络名
#   2. 创建 monitoring 目录结构
#   3. 复制所有配置文件
#   4. 修正 docker-compose 中的网络名
#   5. 拉取镜像并启动
#   6. 验证服务状态
# ============================================================

set -e

echo "========================================"
echo "  Grafana + Prometheus 一键部署"
echo "========================================"

# ---------- 1. 检测 Docker 网络 ----------
echo ""
echo "[1/6] 检测 Docker 网络..."

# 找到 flink-jobmanager 所在的网络
NETWORK=$(docker inspect flink-jobmanager --format '{{range $key, $value := $NetworkSettings.Networks}}{{$key}}{{end}}' 2>/dev/null || echo "")

if [ -z "$NETWORK" ]; then
    # 备选：找 doris-fe 所在的网络
    NETWORK=$(docker inspect doris-fe --format '{{range $key, $value := $NetworkSettings.Networks}}{{$key}}{{end}}' 2>/dev/null || echo "")
fi

if [ -z "$NETWORK" ]; then
    # 备选：找 mysql 所在的网络
    NETWORK=$(docker inspect mysql --format '{{range $key, $value := $NetworkSettings.Networks}}{{$key}}{{end}}' 2>/dev/null || echo "")
fi

if [ -z "$NETWORK" ]; then
    echo "  未找到现有容器网络，将创建新网络 paimon_net"
    docker network create paimon_net 2>/dev/null || true
    NETWORK="paimon_net"
else
    echo "  检测到现有网络: $NETWORK"
fi

# ---------- 2. 创建目录结构 ----------
echo ""
echo "[2/6] 创建目录结构..."

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

mkdir -p prometheus
mkdir -p grafana/provisioning/datasources
mkdir -p grafana/provisioning/dashboards
mkdir -p grafana/dashboards

echo "  目录已创建: $SCRIPT_DIR"

# ---------- 3. 修正 docker-compose 网络名 ----------
echo ""
echo "[3/6] 修正 Docker 网络名为: $NETWORK"

if [ "$NETWORK" != "paimon_net" ]; then
    if [ -f docker-compose.monitoring.yml ]; then
        # Mac 的 sed 需要 -i ''
        if [[ "$OSTYPE" == "darwin"* ]]; then
            sed -i '' "s/paimon_net/$NETWORK/g" docker-compose.monitoring.yml
        else
            sed -i "s/paimon_net/$NETWORK/g" docker-compose.monitoring.yml
        fi
        echo "  已将 paimon_net 替换为 $NETWORK"
    fi
else
    echo "  网络名已是 paimon_net，无需修改"
fi

# 同时修正 docker-compose 中的 external 声明
# 把 external: true 改为已存在的网络名

# ---------- 4. 检查配置文件 ----------
echo ""
echo "[4/6] 检查配置文件..."

FILES=(
    "docker-compose.monitoring.yml"
    "prometheus/prometheus.yml"
    "grafana/provisioning/datasources/datasources.yml"
    "grafana/provisioning/dashboards/dashboards.yml"
    "grafana/dashboards/01-trading-overview.json"
    "grafana/dashboards/02-traffic-analysis.json"
    "grafana/dashboards/03-product-popularity.json"
    "grafana/dashboards/04-user-behavior-funnel.json"
)

ALL_OK=true
for f in "${FILES[@]}"; do
    if [ -f "$f" ]; then
        echo "  [OK] $f"
    else
        echo "  [缺失] $f"
        ALL_OK=false
    fi
done

if [ "$ALL_OK" = false ]; then
    echo ""
    echo "  部分文件缺失！请确保所有文件都在 $SCRIPT_DIR 目录下。"
    exit 1
fi

# ---------- 5. 拉取镜像并启动 ----------
echo ""
echo "[5/6] 拉取镜像并启动服务..."

echo "  拉取 Grafana 镜像..."
docker pull grafana/grafana:11.2.0

echo "  拉取 Prometheus 镜像..."
docker pull prom/prometheus:v2.53.0

echo "  拉取 Node Exporter 镜像..."
docker pull prom/node-exporter:v1.8.2

echo "  启动监控服务..."
docker-compose -f docker-compose.monitoring.yml up -d

# ---------- 6. 验证服务 ----------
echo ""
echo "[6/6] 验证服务状态..."
sleep 5

echo ""
echo "  --- 容器状态 ---"
docker-compose -f docker-compose.monitoring.yml ps

echo ""
echo "========================================"
echo "  部署完成！"
echo "========================================"
echo ""
echo "  Grafana:    http://localhost:3000  (admin / admin)"
echo "  Prometheus: http://localhost:9091  (查看 /targets 确认采集)"
echo ""
echo "  Grafana 看板在左侧菜单 Dashboards → 实时数仓大屏"
echo "  4 块看板已自动加载，无需手动导入"
echo ""
echo "  停止监控:  docker-compose -f docker-compose.monitoring.yml down"
echo "  查看日志:  docker logs -f grafana"
echo "========================================"
