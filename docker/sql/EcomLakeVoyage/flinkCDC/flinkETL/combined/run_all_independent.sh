#!/bin/bash
# ============================================================
# EcomLakeVoyage 实时数仓一键启动脚本（独立作业版）
# 放弃 Statement Set 合并（合并后 1 个作业占多个 slot）
# 改回独立作业逐个提交，每个作业占 1 个 slot
# 8 个 slot 可同时跑 8 个作业，按层级依赖顺序提交
# ============================================================

set -e

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 路径
#   Flink 容器内路径 (sql-client.sh -f 在容器内解析)
CONTAINER_SQL_DIR="/opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL"
#   宿主机路径 (Doris mysql < 在宿主机 shell 解析)
HOST_SQL_DIR="/Users/ok/bigdata/docker/sql/EcomLakeVoyage/flinkCDC/flinkETL"

FLINK_SQL="docker exec -i flink-jobmanager /opt/flink/bin/sql-client.sh"
DORIS_MYSQL="docker exec -i doris-fe mysql -h 127.0.0.1 -P 9030 -u root --default-character-set=utf8mb4"

print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_ok() {
    echo -e "${GREEN}[OK] $1${NC}"
}

print_err() {
    echo -e "${RED}[ERROR] $1${NC}"
}

# 提交 Flink SQL 作业 (不阻塞，后台运行)
submit_flink() {
    local sql_file=$1
    local desc=$2
    echo -e "${YELLOW}  → 提交: ${desc}${NC}"
    if $FLINK_SQL -f "${CONTAINER_SQL_DIR}/${sql_file}" > /tmp/flink_submit.log 2>&1; then
        print_ok "${desc}"
    else
        print_err "${desc} 提交失败"
        cat /tmp/flink_submit.log | tail -20
        # 不退出，继续提交其他作业
    fi
}

# 在 Doris 执行 DDL
exec_doris() {
    local sql_file=$1
    local desc=$2
    echo -e "${YELLOW}  → 执行: ${desc}${NC}"
    if $DORIS_MYSQL < "${HOST_SQL_DIR}/${sql_file}"; then
        print_ok "${desc}"
    else
        print_err "${desc} 执行失败"
    fi
}

# ============================================================
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}EcomLakeVoyage 实时数仓一键启动 (独立作业版)${NC}"
echo -e "${BLUE}========================================${NC}"

# 前置检查
echo -e "\n${YELLOW}[检查] 容器状态...${NC}"
for c in flink-jobmanager flink-taskmanager doris-fe mysql kafka; do
    if ! docker ps --format '{{.Names}}' | grep -q "^$c$"; then
        print_err "容器 $c 未运行"
        exit 1
    fi
done
print_ok "所有容器已运行"

# ============================================================
# 阶段 1: ODS 层 (共 20 个作业，分 3 批提交)
# ============================================================
print_header "阶段 1: ODS 层"

echo -e "\n${YELLOW}[1.1] ODS Batch (4 张 JDBC 维度表, 跑完 FINISHED)${NC}"
for f in ods/base/ods_base_brand.sql ods/base/ods_base_category.sql ods/base/ods_base_dic.sql ods/base/ods_base_region.sql; do
    submit_flink "$f" "ODS batch: $(basename $f .sql)"
done

echo -e "\n${YELLOW}[1.2] ODS CDC (11 张 MySQL CDC 表, 常驻)${NC}"
echo -e "${YELLOW}  分批提交: 先 8 个 (占满 8 slot), 后 3 个${NC}"

# 第 1 批 CDC (8 个, 占满 slot)
for f in ods/base/ods_coupon_info.sql ods/base/ods_sku_info.sql ods/base/ods_spu_info.sql ods/base/ods_user_info.sql \
         ods/business/ods_cart_info.sql ods/business/ods_coupon_use.sql ods/business/ods_order_detail.sql ods/business/ods_order_info.sql; do
    submit_flink "$f" "ODS CDC 批1: $(basename $f .sql)"
done

echo -e "\n${YELLOW}  等待 30 秒让前 8 个作业稳定...${NC}"
sleep 30

# 第 2 批 CDC (3 个)
for f in ods/business/ods_order_status_log.sql ods/business/ods_payment_info.sql ods/business/ods_refund_info.sql; do
    submit_flink "$f" "ODS CDC 批2: $(basename $f .sql)"
done

echo -e "\n${YELLOW}[1.3] ODS Kafka (5 张日志表, 常驻)${NC}"
for f in ods/log/ods_log_startup.sql ods/log/ods_log_page_view.sql ods/log/ods_log_action.sql ods/log/ods_log_display.sql ods/log/ods_log_error.sql; do
    submit_flink "$f" "ODS Kafka: $(basename $f .sql)"
done

# ============================================================
# 阶段 2: DIM 层 (8 个作业)
# ============================================================
print_header "阶段 2: DIM 层"

echo -e "\n${YELLOW}[2.1] DIM Batch (4 张维度表, 依赖 ODS batch)${NC}"
for f in dim/batch_base/dim_base_brand.sql dim/batch_base/dim_base_category.sql dim/batch_base/dim_base_dic.sql dim/batch_base/dim_base_region.sql; do
    submit_flink "$f" "DIM batch: $(basename $f .sql)"
done

echo -e "\n${YELLOW}[2.2] DIM Streaming (4 张维度表, 依赖 ODS CDC)${NC}"
for f in dim/streaming_base/dim_coupon_info.sql dim/streaming_base/dim_spu_info.sql dim/streaming_base/dim_sku_info.sql dim/streaming_base/dim_user_info.sql; do
    submit_flink "$f" "DIM streaming: $(basename $f .sql)"
done

# ============================================================
# 阶段 3: DWD 层 (12 个作业)
# ============================================================
print_header "阶段 3: DWD 层"

echo -e "\n${YELLOW}[3.1] DWD Business (7 张宽表, 依赖 DIM)${NC}"
echo -e "${YELLOW}  分批提交: 先 4 个, 后 3 个${NC}"
for f in dwd/business/dwd_cart_info.sql dwd/business/dwd_coupon_use.sql dwd/business/dwd_order_detail.sql dwd/business/dwd_order_info.sql; do
    submit_flink "$f" "DWD business 批1: $(basename $f .sql)"
done
echo -e "\n${YELLOW}  等待 20 秒...${NC}"
sleep 20
for f in dwd/business/dwd_order_status_log.sql dwd/business/dwd_payment_info.sql dwd/business/dwd_refund_info.sql; do
    submit_flink "$f" "DWD business 批2: $(basename $f .sql)"
done

echo -e "\n${YELLOW}[3.2] DWD Log (5 张宽表, 依赖 DIM + ODS Kafka)${NC}"
for f in dwd/log/dwd_log_startup.sql dwd/log/dwd_log_page_view.sql dwd/log/dwd_log_action.sql dwd/log/dwd_log_display.sql dwd/log/dwd_log_error.sql; do
    submit_flink "$f" "DWD log: $(basename $f .sql)"
done

# ============================================================
# 阶段 4: Doris DDL (不占 slot)
# ============================================================
print_header "阶段 4: Doris DDL"

exec_doris "dws/business/doris_dws_init.sql" "DWS 业务表建表"
exec_doris "dws/log/doris_dws_log_init.sql" "DWS 日志表建表"

# ============================================================
# 阶段 5: DWS 聚合 (5 个 Flink 作业)
# ============================================================
print_header "阶段 5: DWS 聚合作业"

echo -e "\n${YELLOW}[5.1] DWS Trade (2 个业务聚合)${NC}"
for f in dws/business/dws_trade_user_stats.sql dws/business/dws_trade_sku_stats.sql; do
    submit_flink "$f" "DWS trade: $(basename $f .sql)"
done

echo -e "\n${YELLOW}[5.2] DWS Log (3 个日志聚合)${NC}"
for f in dws/log/dws_log_page_stats.sql dws/log/dws_log_sku_exposure_stats.sql dws/log/dws_log_user_action_stats.sql; do
    submit_flink "$f" "DWS log: $(basename $f .sql)"
done

# ============================================================
# 阶段 6: Doris 视图
# ============================================================
print_header "阶段 6: Doris 视图 + 物化视图"

exec_doris "dws/business/doris_dws_views.sql" "DWS 物化视图"
exec_doris "ads/doris_ads_all_in_one.sql" "ADS 视图 (28 个视图 + 1 个 MV)"

# ============================================================
# 完成
# ============================================================
print_header "全部提交完成!"

echo -e "作业统计:"
echo -e "  ODS: 20 个 (4 batch + 11 CDC + 5 Kafka)"
echo -e "  DIM:  8 个 (4 batch + 4 streaming)"
echo -e "  DWD: 12 个 (7 business + 5 log)"
echo -e "  DWS:  5 个 (2 trade + 3 log)"
echo -e "  Doris DDL + 视图已创建"
echo ""
echo -e "${YELLOW}注意: 作业数超过 slot 数, 部分作业会排队等待${NC}"
echo -e "${YELLOW}      Flink 会自动调度, 不需要手动干预${NC}"
echo ""
echo -e "${BLUE}监控:${NC}"
echo -e "  Flink Web UI: http://localhost:8081"
echo -e "  Doris Web UI: http://localhost:8030"
