#!/bin/bash
# ============================================================
# EcomLakeVoyage 实时数仓一键启动脚本（Statement Set 合并版）
# 作用：按依赖顺序提交 7 个 Flink SQL 作业 + Doris DDL
# 占用 slot 峰值：8（匹配 config.yaml 中 numberOfTaskSlots=8）
# ============================================================

set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 路径说明:
#   - CONTAINER 路径给 Flink sql-client.sh -f 用 (在容器内解析)
#   - HOST 路径给 Doris mysql < 重定向用 (在宿主机 shell 解析, stdin 通过 -i 传入容器)
CONTAINER_SQL_DIR="/opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL"
HOST_SQL_DIR="/Users/ok/bigdata/docker/sql/EcomLakeVoyage/flinkCDC/flinkETL"

COMBINED_DIR="${CONTAINER_SQL_DIR}/combined"
DORIS_DDL_DIR="${HOST_SQL_DIR}/dws"
ADS_DIR="${HOST_SQL_DIR}/ads"

# Flink SQL Client 命令
FLINK_SQL_CLIENT="docker exec -i flink-jobmanager /opt/flink/bin/sql-client.sh"
# Doris MySQL Client 命令 (stdin 通过 docker exec -i 从宿主机传入)
DORIS_MYSQL="docker exec -i doris-fe mysql -h 127.0.0.1 -P 9030 -u root --default-character-set=utf8mb4"

# 计数器
STEP=0
TOTAL=12

# ============================================================
# 函数定义
# ============================================================

print_step() {
    STEP=$((STEP+1))
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}[${STEP}/${TOTAL}] $1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_ok() {
    echo -e "${GREEN}[OK] $1${NC}"
}

print_warn() {
    echo -e "${YELLOW}[WARN] $1${NC}"
}

print_err() {
    echo -e "${RED}[ERROR] $1${NC}"
}

# 检查容器是否运行
check_container() {
    if ! docker ps --format '{{.Names}}' | grep -q "^$1$"; then
        print_err "容器 $1 未运行，请先 docker compose up -d"
        exit 1
    fi
}

# 提交 Flink SQL 文件
submit_flink_sql() {
    local sql_file=$1
    local job_name=$2
    print_step "提交 Flink 作业: ${job_name}"
    echo -e "  文件: ${sql_file}"
    if $FLINK_SQL_CLIENT -f "$sql_file"; then
        print_ok "${job_name} 提交成功"
    else
        print_err "${job_name} 提交失败"
        exit 1
    fi
}

# 在 Doris 执行 SQL
exec_doris_sql() {
    local sql_file=$1
    local desc=$2
    print_step "执行 Doris DDL: ${desc}"
    echo -e "  文件: ${sql_file}"
    if $DORIS_MYSQL < "$sql_file"; then
        print_ok "${desc} 执行成功"
    else
        print_err "${desc} 执行失败"
        exit 1
    fi
}

# ============================================================
# 前置检查
# ============================================================
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}EcomLakeVoyage 实时数仓一键启动${NC}"
echo -e "${BLUE}========================================${NC}"

echo -e "\n${YELLOW}[检查] 容器状态...${NC}"
check_container "flink-jobmanager"
check_container "flink-taskmanager"
check_container "doris-fe"
check_container "mysql"
check_container "kafka"
print_ok "所有必需容器已运行"

# ============================================================
# 阶段 1: ODS 层（3 个作业，可并行提交）
# ============================================================
print_step "阶段 1: ODS 层 - 启动 3 个合并作业"

echo -e "\n${YELLOW}[1.1] ODS 批量同步（4 张 JDBC 维度表，跑完 FINISHED）${NC}"
submit_flink_sql "${COMBINED_DIR}/ods_batch_all.sql" "ODS Batch (JDBC 4 tables)"

echo -e "\n${YELLOW}[1.2] ODS CDC 实时同步（11 张 MySQL CDC 表，常驻）${NC}"
submit_flink_sql "${COMBINED_DIR}/ods_cdc_all.sql" "ODS CDC (MySQL CDC 11 tables)"

echo -e "\n${YELLOW}[1.3] ODS Kafka 日志同步（5 张 Kafka 日志表，常驻）${NC}"
submit_flink_sql "${COMBINED_DIR}/ods_kafka_all.sql" "ODS Kafka (5 log tables)"

# ============================================================
# 阶段 2: DIM 层（2 个作业）
# ============================================================
echo -e "\n${YELLOW}[提示] 等待 ODS batch 作业 FINISHED 后再提交 DIM batch...${NC}"
echo -e "${YELLOW}[提示] DIM streaming 依赖 ODS CDC，可立即提交（流式不会等数据）${NC}"

print_step "阶段 2: DIM 层 - 启动 2 个合并作业"

echo -e "\n${YELLOW}[2.1] DIM 批量同步（4 张维度表，跑完 FINISHED）${NC}"
submit_flink_sql "${COMBINED_DIR}/dim_batch_all.sql" "DIM Batch (4 dim tables)"

echo -e "\n${YELLOW}[2.2] DIM 流式同步（4 张维度表，常驻）${NC}"
submit_flink_sql "${COMBINED_DIR}/dim_streaming_all.sql" "DIM Streaming (4 dim tables)"

# ============================================================
# 阶段 3: DWD 层（2 个作业）
# ============================================================
echo -e "\n${YELLOW}[提示] DWD 依赖 DIM 层，可立即提交（流式不会等数据）${NC}"

print_step "阶段 3: DWD 层 - 启动 2 个合并作业"

echo -e "\n${YELLOW}[3.1] DWD 业务宽表（7 张宽表，常驻）${NC}"
submit_flink_sql "${COMBINED_DIR}/dwd_business_all.sql" "DWD Business (7 wide tables)"

echo -e "\n${YELLOW}[3.2] DWD 日志宽表（5 张宽表，常驻）${NC}"
submit_flink_sql "${COMBINED_DIR}/dwd_log_all.sql" "DWD Log (5 wide tables)"

# ============================================================
# 阶段 4: Doris DDL（建表，不占 slot）
# ============================================================
print_step "阶段 4: Doris DDL - 建 DWS 目标表"

echo -e "\n${YELLOW}[4.1] DWS 业务表建表${NC}"
exec_doris_sql "${DORIS_DDL_DIR}/business/doris_dws_init.sql" "DWS Business DDL"

echo -e "\n${YELLOW}[4.2] DWS 日志表建表${NC}"
exec_doris_sql "${DORIS_DDL_DIR}/log/doris_dws_log_init.sql" "DWS Log DDL"

# ============================================================
# 阶段 5: DWS 聚合作业（1 个合并作业，5 个 sink）
# ============================================================
print_step "阶段 5: DWS 聚合作业 - 启动 1 个合并作业（5 个 sink）"

echo -e "\n${YELLOW}[5.1] DWS 聚合（5 个 DWS sink 合并为 1 个作业，常驻）${NC}"
submit_flink_sql "${COMBINED_DIR}/dws_flink_all.sql" "DWS Aggregation (5 sinks)"

# ============================================================
# 阶段 6: Doris 视图 + 物化视图
# ============================================================
print_step "阶段 6: Doris 视图 - 建物化视图 + ADS 视图"

echo -e "\n${YELLOW}[6.1] DWS 物化视图 + 中间视图${NC}"
exec_doris_sql "${DORIS_DDL_DIR}/business/doris_dws_views.sql" "DWS Materialized Views"

echo -e "\n${YELLOW}[6.2] ADS 视图（28 个普通视图 + 1 个物化视图）${NC}"
exec_doris_sql "${ADS_DIR}/doris_ads_all_in_one.sql" "ADS Views"

# ============================================================
# 完成
# ============================================================
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}全部作业已提交!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "作业概览:"
echo -e "  ODS 层: 3 个作业 (1 batch FINISHED + 2 streaming 常驻)"
echo -e "  DIM 层: 2 个作业 (1 batch FINISHED + 1 streaming 常驻)"
echo -e "  DWD 层: 2 个作业 (streaming 常驻)"
echo -e "  DWS 层: 1 个作业 (streaming 常驻, 5 个 Doris sink)"
echo -e "  Doris:  DDL + 视图已创建"
echo ""
echo -e "常驻作业数: 6 (ODS CDC + ODS Kafka + DIM streaming + DWD business + DWD log + DWS)"
echo -e "Slot 占用: 6/8 (留 2 个余量)"
echo ""
echo -e "${BLUE}监控地址:${NC}"
echo -e "  Flink Web UI:    http://localhost:8081"
echo -e "  Doris Web UI:    http://localhost:8030"
echo -e "  Grafana:         http://localhost:3000  (admin/admin)"
echo ""
