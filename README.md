# EcomLakeVoyage

> 基于 Flink + Paimon + Doris 的一站式电商实时数仓：从 MySQL CDC/Kafka 日志采集，到 Paimon 湖存储 ODS/DIM/DWD，再到 Flink 聚合写入 Doris DWS/ADS，配套 Prometheus + Grafana 监控与 4 块可视化分析大屏。

[![Flink](https://img.shields.io/badge/Flink-1.20.0-blue?logo=apache-flink&logoColor=white)](https://flink.apache.org/)
[![Paimon](https://img.shields.io/badge/Paimon-1.0.0-green)](https://paimon.apache.org/)
[![Doris](https://img.shields.io/badge/Doris-3.1.3-blueviolet?logo=apachedoris&logoColor=white)](https://doris.apache.org/)
[![Kafka](https://img.shields.io/badge/Kafka-3.9.2-black?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.0-orange?logo=mysql&logoColor=white)](https://www.mysql.com/)
[![Python](https://img.shields.io/badge/Python-3.11-yellow?logo=python&logoColor=white)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker&logoColor=white)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](https://github.com/)
[![Version](https://img.shields.io/badge/version-1.0.0-orange.svg)](https://github.com/)

---

## 目录

- [架构总览](#架构总览)
- [技术栈](#技术栈)
- [快速开始](#快速开始)
- [最小用法示例](#最小用法示例)
- [配置项](#配置项)
- [数仓分层详解](#数仓分层详解)
- [监控与可视化](#监控与可视化)
- [贡献指南](#贡献指南)
- [Roadmap](#roadmap)
- [License](#license)

---

## 架构总览

```
┌──────────────┐     CDC      ┌──────────────┐   Lookup Join   ┌──────────────┐
│   MySQL 8.0  │─────────────▶│  Paimon ODS  │────────────────▶│  Paimon DWD  │
│ (业务/维度库) │              │  (湖存储)     │                 │  (事实宽表)   │
└──────────────┘              └──────────────┘                 └──────┬───────┘
                                                                      │
┌──────────────┐   Append-Only  ┌──────────────┐                      │
│  Kafka 3.9   │───────────────▶│ Paimon ODS   │                      │
│  (日志流)    │                │ (Log 层)      │                      │
└──────────────┘                └──────────────┘                      │
                                                                      ▼
                              ┌─────────────────────────────────────────────────┐
                              │              Flink 1.20 聚合引擎                 │
                              │  (流式 ETL / Window / GROUP BY / JOIN)          │
                              └──────────────────────┬──────────────────────────┘
                                                     │ Stream Load
                                                     ▼
                              ┌─────────────────────────────────────────────────┐
                              │         Doris 3.1.3 OLAP 引擎                    │
                              │  DWS (汇总表 + 物化视图 MV) → ADS (分析视图)     │
                              └──────────────────────┬──────────────────────────┘
                                                     │ MySQL 协议
                                                     ▼
                              ┌─────────────────────────────────────────────────┐
                              │   Grafana 11.2.0 可视化大屏 (4 块预设看板)        │
                              │   Prometheus 2.54 指标采集 (Flink/Doris/Host)    │
                              └─────────────────────────────────────────────────┘
```

**数据流向（2 条主链路）**：

1. **业务 CDC 链路**：MySQL (`gmall_base` / `gmall_business`) → Debezium Connect → Flink MySQL CDC → Paimon ODS → Paimon DIM/DWD (Lookup Join 拼宽) → Flink 聚合 → Doris DWS → Doris ADS 视图
2. **日志流链路**：埋点日志 → Kafka (topic_base_log / topic_action / ...) → Flink Kafka Source → Paimon ODS (Append-Only) → Paimon DWD (炸裂/清洗) → Flink 聚合 → Doris DWS → Doris ADS 视图

---

## 技术栈

| 层级 | 组件 | 版本 | 作用 |
|------|------|------|------|
| **数据源** | MySQL | 8.0 | 业务库(gmall_business) + 维度库(gmall_base) |
| | Kafka | 3.9.2 | 埋点日志消息队列 (KRaft 模式) |
| | Debezium Connect | 2.5 | MySQL Binlog CDC 连接器 |
| **湖存储** | Apache Paimon | 1.0.0 | ODS/DIM/DWD 三层湖仓，changelog-producer=input |
| **计算引擎** | Apache Flink | 1.20.0 | PyFlink + Flink SQL，流式 ETL 聚合 |
| **OLAP 引擎** | Apache Doris | 3.1.3 | FE + BE，DWS Unique 表 + MV 物化视图 |
| **监控** | Prometheus | 2.54.1 | Flink/Doris/Host 指标采集 |
| | Grafana | 11.2.0 | 4 块预设分析看板 (交易/商品/流量/地区) |
| **部署** | Docker | 24.0+ | 容器化一键部署 |
| **数据生成** | Python | 3.11 | Faker + NumPy 高逼真模拟数据 |

---

## 快速开始

### 0. 环境要求

```bash
Docker >= 24.0  (含 Docker Compose v2)
Python >= 3.11  (仅本地生成模拟数据时需要)
内存 >= 8GB     (Doris BE + Flink TM 建议 16GB)
磁盘空闲 >= 20GB
```

### 1. 克隆项目并启动基础服务

```bash
git clone https://github.com/your-username/EcomLakeVoyage.git
cd EcomLakeVoyage

# 一键启动: Kafka / MySQL / Debezium / Flink / Doris
bash start-docker.sh
```

启动完成后，各服务访问地址：

| 服务 | 地址 | 账号/密码 |
|------|------|-----------|
| Flink Web UI | http://localhost:8081 | (无) |
| Kafka UI | http://localhost:8080 | (无) |
| Doris FE Web UI | http://localhost:8030 | root / (空) |
| Doris MySQL 协议 | `mysql -h 127.0.0.1 -P 9030 -u root` | root / (空) |
| MySQL | `mysql -h 127.0.0.1 -P 3306 -u root -p` | root / 123456 |

### 2. 生成 MySQL 模拟数据

```bash
# 安装依赖 (仅第一次, 推荐 venv 或 conda 环境)
pip install -r requirements.txt

# 预览数据不写入
python3 docker/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/gmall_mock_data_generator.py \
  --dry-run --seed 42

# 写入 MySQL (容器运行时, host=localhost, 或容器内 host=mysql)
python3 docker/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/gmall_mock_data_generator.py \
  --host localhost --port 3306 -u root -p 123456 \
  --users 1000 --spus 200 --orders 10000
```

预期输出（摘要）：

```
============================================================
  gmall 模拟数据生成器 - 写入模式
============================================================
  用户数:    1000
  SPU 数:    200   SKU 数: ~700
  订单数:    10000
  随机种子:  42
------------------------------------------------------------
[1/8] 写入 base_dic ...        45 行  ✓
[2/8] 写入 base_region ...     340 行 ✓
[3/8] 写入 base_brand ...      42 行  ✓
[4/8] 写入 base_category ...   180 行 ✓
[5/8] 写入 coupon_info ...     20 行  ✓
[6/8] 写入 user_info ...       1000 行 ✓
[7/8] 写入 spu_info+sku ...    200+685 行 ✓
[8/8] 写入订单链路(order/detail/status_log/payment/refund/coupon_use/cart) ...
      order_info:          10000 行 ✓
      order_detail:        15230 行 ✓
      order_status_log:    38420 行 ✓
      payment_info:        8920 行  ✓
      refund_info:         1340 行  ✓
      coupon_use:          5680 行  ✓
      cart_info:           8000 行  ✓
============================================================
  ✅ 全部写入完成！总写入行数: 约 9 万
============================================================
```

### 3. 初始化 Kafka 日志 Topic 并启动日志模拟器

```bash
# 创建 5 个日志 topic (startup / page_view / action / display / error)
bash docker/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/log/kafka_topic_init.sh

# 启动流式日志模拟器 (每秒产生 ~100 条日志, 发送到 Kafka)
python3 docker/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/gmall_kafka_log_generator.py \
  --bootstrap-servers localhost:9092 \
  --rate 100 --duration 3600
```

### 4. 按顺序提交 Flink SQL ETL 作业

```bash
# ---- 第一阶段：ODS 层 CDC 同步 (8 张基础表 + 7 张业务表 + 5 张日志表) ----
# 基础维度表
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/base/ods_base_brand.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/base/ods_base_category.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/base/ods_base_dic.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/base/ods_base_region.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/base/ods_coupon_info.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/base/ods_spu_info.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/base/ods_sku_info.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/base/ods_user_info.sql

# 业务表 CDC
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/business/ods_cart_info.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/business/ods_coupon_use.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/business/ods_order_detail.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/business/ods_order_info.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/business/ods_order_status_log.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/business/ods_payment_info.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/business/ods_refund_info.sql

# 日志 Kafka 同步
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/log/ods_log_startup.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/log/ods_log_page_view.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/log/ods_log_action.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/log/ods_log_display.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/log/ods_log_error.sql

# ---- 第二阶段：DIM 层维度表拼宽 ----
# Batch 维度 (一次性)
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dim/batch_base/dim_base_brand.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dim/batch_base/dim_base_category.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dim/batch_base/dim_base_dic.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dim/batch_base/dim_base_region.sql

# Streaming 维度 (持续运行)
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dim/streaming_base/dim_coupon_info.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dim/streaming_base/dim_spu_info.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dim/streaming_base/dim_sku_info.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dim/streaming_base/dim_user_info.sql

# ---- 第三阶段：DWD 层事实宽表拼宽 ----
# 业务事实表
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/business/dwd_cart_info.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/business/dwd_coupon_use.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/business/dwd_order_detail.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/business/dwd_order_info.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/business/dwd_order_status_log.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/business/dwd_payment_info.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/business/dwd_refund_info.sql

# 日志事实表 (含曝光炸裂)
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/log/dwd_log_startup.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/log/dwd_log_page_view.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/log/dwd_log_action.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/log/dwd_log_display.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/log/dwd_log_error.sql

# ---- 第四阶段：DWS 层 Flink → Doris 聚合 + Doris 初始化 ----
# Doris DWS 表初始化 (Unique Key 表 + 物化视图)
docker cp docker/sql/EcomLakeVoyage/flinkCDC/flinkETL/dws/business/doris_dws_init.sql mysql:/tmp/
docker exec -i mysql sh -c 'mysql -h doris-fe -P 9030 -u root < /tmp/doris_dws_init.sql'

docker cp docker/sql/EcomLakeVoyage/flinkCDC/flinkETL/dws/log/doris_dws_log_init.sql mysql:/tmp/
docker exec -i mysql sh -c 'mysql -h doris-fe -P 9030 -u root < /tmp/doris_dws_log_init.sql'

# 启动 Flink DWS 聚合作业 (读 Paimon DWD → 写 Doris)
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dws/business/dws_trade_user_stats.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dws/business/dws_trade_sku_stats.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dws/log/dws_log_page_stats.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dws/log/dws_log_sku_exposure_stats.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dws/log/dws_log_user_action_stats.sql

# 建 Doris 物化视图 & DWS 视图
docker cp docker/sql/EcomLakeVoyage/flinkCDC/flinkETL/dws/business/doris_dws_views.sql mysql:/tmp/
docker exec -i mysql sh -c 'mysql -h doris-fe -P 9030 -u root < /tmp/doris_dws_views.sql'

# ---- 第五阶段：ADS 层分析视图（5 大模型 + 13 个视图） ----
docker cp docker/sql/EcomLakeVoyage/flinkCDC/flinkETL/ads/doris_ads_all_in_one.sql mysql:/tmp/
docker exec -i mysql sh -c 'mysql -h doris-fe -P 9030 -u root < /tmp/doris_ads_all_in_one.sql'
```

### 5. 部署监控栈 (Prometheus + Grafana)

```bash
cd docker/monitoring
bash setup-monitoring.sh
```

部署完成后：
- **Grafana**: http://localhost:3000 (admin / admin)，默认首页已设为交易总览大屏
- **Prometheus**: http://localhost:9090 (/targets 查看采集状态)

---

## 最小用法示例

### 示例 1：查询今日 GMV 实时大屏（ADS 层）

连接 Doris：

```bash
mysql -h 127.0.0.1 -P 9030 -u root
```

执行：

```sql
SELECT * FROM ads.gmv_today_summary;
```

**预期输出**：

```
+------------+-------------------+-------------+----------------------+---------------------+--------------------------+------------+-----------------+------------------+
| dt         | total_order_count | total_gmv   | total_payment_amount | total_refund_amount | payment_conversion_rate  | refund_rate| avg_order_amount| coupon_usage_rate |
+------------+-------------------+-------------+----------------------+---------------------+--------------------------+------------+-----------------+------------------+
| 2026-08-22 |              1247 | 1285430.50  |           1154320.25 |            34520.80 |                    88.45 |       2.72 |         1030.82 |             6.38 |
+------------+-------------------+-------------+----------------------+---------------------+--------------------------+------------+-----------------+------------------+
```

### 示例 2：查询 RFM 用户分层分布

```sql
SELECT user_segment, user_count, user_pct, total_amount, amount_pct
FROM ads.rfm_segment_summary
ORDER BY total_amount DESC;
```

**预期输出**：

```
+------------------+------------+-----------+--------------+------------+
| user_segment     | user_count | user_pct  | total_amount | amount_pct |
+------------------+------------+-----------+--------------+------------+
| 高价值用户        |        186 |     18.60 |   4852340.50 |      52.34 |
| 重点保持用户      |         92 |      9.20 |   1325430.20 |      14.31 |
| 重点发展用户      |        134 |     13.40 |   1085720.80 |      11.72 |
| 重点挽留用户      |         58 |      5.80 |    584320.40 |       6.30 |
| 一般价值用户      |        142 |     14.20 |    720350.90 |       7.77 |
| 一般保持用户      |         78 |      7.80 |    320150.30 |       3.45 |
| 一般发展用户      |        120 |     12.00 |    298640.10 |       3.22 |
| 流失预警用户      |         90 |      9.00 |     80230.40 |       0.86 |
+------------------+------------+-----------+--------------+------------+
```

### 示例 3：查询 TOP 10 热门用户路径

```sql
SELECT path, transition_count, transition_users
FROM ads.user_path_top10
WHERE dt = CURDATE()
ORDER BY rn ASC;
```

**预期输出**：

```
+------------------------------------------+------------------+------------------+
| path                                     | transition_count | transition_users |
+------------------------------------------+------------------+------------------+
| home -> category_list                    |            18324 |             6234 |
| category_list -> sku_detail              |            12093 |             4821 |
| sku_detail -> cart                       |             7284 |             3012 |
| cart -> checkout                         |             4823 |             2204 |
| checkout -> pay_success                  |             3920 |             1823 |
| sku_detail -> sku_detail                 |             2831 |             1523 |
| category_list -> category_list           |             2412 |             1402 |
| home -> search_result                    |             2103 |             1204 |
| search_result -> sku_detail              |             1832 |              982 |
| sku_detail -> home                       |             1203 |              782 |
+------------------------------------------+------------------+------------------+
```

---

## 配置项

### 环境变量

在 [docker-compose.yml](file:///Users/ok/bigdata/docker/docker-compose.yml) 中可配置的核心环境变量：

| 变量名 | 默认值 | 说明 | 所属服务 |
|--------|--------|------|----------|
| `MYSQL_ROOT_PASSWORD` | `123456` | MySQL root 密码 | mysql |
| `MYSQL_DATABASE` | `demo` | MySQL 默认库 | mysql |
| `TZ` | `Asia/Shanghai` | 全局时区 | 全部服务 |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `true` | 自动创建 Kafka Topic | kafka |
| `KAFKA_DEFAULT_TOPIC_PARTITIONS` | `1` | 默认分区数 | kafka |
| `FE_SERVERS` | `fe1:172.22.0.10:9010` | Doris FE 集群地址 | doris-fe/doris-be |
| `GF_SECURITY_ADMIN_USER` | `admin` | Grafana 管理员账号 | grafana |
| `GF_SECURITY_ADMIN_PASSWORD` | `admin` | Grafana 管理员密码 | grafana |

### Paimon 表核心配置 (Flink SQL 中)

```sql
-- 所有 Paimon 表统一的生产级配置
WITH (
    'changelog-producer'       = 'input',         -- 产完整 changelog 供下游 temporal join
    'merge-engine'             = 'deduplicate',   -- PK 去重合并
    'bucket'                   = '4',              -- bucket 数 (维度表 4, 事实表可调 8-16)
    'target-file-size'         = '128mb',          -- 目标文件大小
    'snapshot.time-retained'   = '7d',             -- 快照保留 7 天
    'snapshot.num-retained.min' = '10',            -- 至少保留 10 个快照
    'snapshot.num-retained.max' = '20',            -- 最多保留 20 个快照
    'changelog.time-retained'  = '7d',             -- changelog 保留 7 天
    'file.format'              = 'orc',            -- ORC 列存
    'orc.compression'          = 'zstd'            -- ZSTD 压缩
)
```

### Flink Checkpoint 配置 (统一 20+ 作业一致)

```sql
SET 'execution.checkpointing.interval'                            = '10s';
SET 'execution.checkpointing.mode'                               = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout'                            = '10min';
SET 'execution.checkpointing.min-pause'                          = '30s';
SET 'execution.checkpointing.max-concurrent-checkpoints'         = '1';
SET 'execution.checkpointing.externalized-checkpoint-retention'  = 'RETAIN_ON_CANCELLATION';
SET 'execution.checkpointing.tolerable-failed-checkpoints'       = '3';
SET 'state.backend'                                              = 'hashmap';
SET 'state.checkpoints.dir'         = 'file:///opt/flink/paimon_warehouse/flink-checkpoints';
SET 'state.savepoints.dir'          = 'file:///opt/flink/paimon_warehouse/flink-savepoints';
SET 'restart-strategy'               = 'failure-rate';
SET 'table.exec.state.ttl'          = '24h';
SET 'table.local-time-zone'         = 'Asia/Shanghai';
SET 'parallelism.default'           = '1';
```

### Doris DWS 表配置 (Unique Key 模型)

```sql
-- 所有 DWS 汇总表统一配置
CREATE TABLE dws.trade_user_stats (
    ...
) ENGINE=OLAP
UNIQUE KEY(user_id, dt)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES (
    "replication_allocation"          = "tag.location.default:1",
    "enable_unique_key_merge_on_write"= "true",   -- MOW 模式，changelog 实时 upsert
    "compaction_policy"               = "TIME_SERIES"
);
```

---

## 数仓分层详解

```
sql/EcomLakeVoyage/flinkCDC/flinkETL/
├── ods/                                    # ODS 层 (20 张表)
│   ├── base/                               # 基础维度 CDC (8 张)
│   │   ├── ods_base_brand.sql              #   品牌表
│   │   ├── ods_base_category.sql           #   三级分类表
│   │   ├── ods_base_dic.sql                #   数据字典
│   │   ├── ods_base_region.sql             #   省/市/区 + 大区
│   │   ├── ods_coupon_info.sql             #   优惠券模板
│   │   ├── ods_spu_info.sql                #   SPU 商品
│   │   ├── ods_sku_info.sql                #   SKU 商品
│   │   └── ods_user_info.sql               #   用户表
│   ├── business/                           # 业务表 CDC (7 张)
│   │   ├── ods_cart_info.sql               #   购物车
│   │   ├── ods_coupon_use.sql              #   优惠券领用
│   │   ├── ods_order_detail.sql            #   订单明细
│   │   ├── ods_order_info.sql              #   订单主表
│   │   ├── ods_order_status_log.sql        #   订单状态履历 (事件源)
│   │   ├── ods_payment_info.sql            #   支付记录
│   │   └── ods_refund_info.sql             #   退款记录
│   ├── log/                                # 日志 Kafka 同步 (5 张, Append-Only)
│   │   ├── ods_log_startup.sql             #   App 启动日志
│   │   ├── ods_log_page_view.sql           #   页面浏览 (PV)
│   │   ├── ods_log_action.sql              #   用户动作 (点击/加购)
│   │   ├── ods_log_display.sql             #   商品曝光 (含 array 炸裂)
│   │   └── ods_log_error.sql               #   错误/崩溃日志
│   ├── gmall_mock_data_generator.py        #   ⭐ MySQL 全量表高逼真数据生成器
│   ├── gmall_kafka_log_generator.py        #   ⭐ Kafka 日志流模拟器
│   ├── gmall_streaming_simulator.py        #   ⭐ 订单状态流式推进器
│   └── mysql_mock_data_generator.py        #   MySQL 基础版数据生成
│
├── dim/                                    # DIM 层 (8 张维度宽表)
│   ├── batch_base/                         # 一次性批量维度
│   │   ├── dim_base_brand.sql              #   品牌维度 (冗余分类信息)
│   │   ├── dim_base_category.sql           #   分类维度 (三级拼接)
│   │   ├── dim_base_dic.sql                #   字典维度 (code -> name 映射)
│   │   └── dim_base_region.sql             #   地区维度 (省/市/区 + 大区)
│   └── streaming_base/                     # 持续更新维度
│       ├── dim_coupon_info.sql             #   优惠券维度
│       ├── dim_sku_info.sql                #   SKU 维度 (拼 SPU/分类/品牌)
│       ├── dim_spu_info.sql                #   SPU 维度
│       └── dim_user_info.sql               #   用户维度
│
├── dwd/                                    # DWD 层 (12 张事实宽表)
│   ├── business/                           # 业务事实 (7 张)
│   │   ├── dwd_cart_info.sql               #   购物车 (拼用户/SKU/分类)
│   │   ├── dwd_coupon_use.sql              #   优惠券核销
│   │   ├── dwd_order_detail.sql            #   订单明细 (拼 SKU/SPU/品牌/分类)
│   │   ├── dwd_order_info.sql              #   订单主表 (拼用户/地区)
│   │   ├── dwd_order_status_log.sql        #   订单状态 (漏斗/状态机)
│   │   ├── dwd_payment_info.sql            #   支付记录
│   │   └── dwd_refund_info.sql             #   退款记录
│   └── log/                                # 日志事实 (5 张)
│       ├── dwd_log_startup.sql             #   启动 (拼用户)
│       ├── dwd_log_page_view.sql           #   页面浏览 (PV/UV/时长)
│       ├── dwd_log_action.sql              #   动作日志
│       ├── dwd_log_display.sql             #   曝光日志 (炸裂 + 拼 SKU)
│       └── dwd_log_error.sql               #   错误日志
│
├── dws/                                    # DWS 层 (聚合 + Doris MV)
│   ├── business/                           # 交易域
│   │   ├── doris_dws_init.sql              #   Doris DWS 表建表 DDL
│   │   ├── doris_dws_views.sql             #   Doris 物化视图 + 中间视图
│   │   ├── dws_trade_user_stats.sql        #   用户交易日汇总 (Flink → Doris)
│   │   └── dws_trade_sku_stats.sql         #   商品交易日汇总
│   └── log/                                # 日志域
│       ├── doris_dws_log_init.sql          #   Doris 日志 DWS 表 DDL
│       ├── dws_log_page_stats.sql          #   页面流量日汇总
│       ├── dws_log_sku_exposure_stats.sql  #   SKU 曝光日汇总
│       └── dws_log_user_action_stats.sql   #   用户活跃日汇总
│
└── ads/                                    # ADS 层 (5 大分析模型, 约 20+ 视图)
    └── doris_ads_all_in_one.sql            #   ⭐ 一键初始化脚本
                                            #   基础 7 视图 + 日志 6 视图 + 5 模型
                                            #   模型: 1-留存 2-路径 3-RFM 4-GMV大屏 5-商品趋势
```

---

## 监控与可视化

### 预设 Grafana 看板 (4 块，自动加载)

| 编号 | 看板名称 | 文件 | 核心指标 |
|------|----------|------|----------|
| 01 | 📈 交易总览大屏 | [01-trading-overview.json](file:///Users/ok/bigdata/docker/monitoring/grafana/dashboards/01-trading-overview.json) | 今日 GMV / 订单数 / 支付转化率 / 退款率 / 客单价 / 小时级趋势 |
| 02 | 🛍️ 商品分析 | [02-product-analysis.json](file:///Users/ok/bigdata/docker/monitoring/grafana/dashboards/02-product-analysis.json) | TOP SKU / TOP 品牌 / 品类占比 / 7 日均线 / 异常预警 |
| 03 | 🚦 流量漏斗 | [03-traffic-funnel.json](file:///Users/ok/bigdata/docker/monitoring/grafana/dashboards/03-traffic-funnel.json) | 浏览→加购→下单→支付 转化率 / 热门路径 TOP10 / 跳出率 |
| 04 | 🗺️ 用户地区分布 | [04-user-region.json](file:///Users/ok/bigdata/docker/monitoring/grafana/dashboards/04-user-region.json) | 省份热力 / 大区对比 / 城市排行 / 地区客单价差异 |

### Prometheus 采集目标

见 [prometheus.yml](file:///Users/ok/bigdata/docker/monitoring/prometheus/prometheus.yml)：

| Job | Target | Metrics 端口 |
|-----|--------|--------------|
| flink-jobmanager | `flink-jobmanager:8081` | `/jobmanager/metrics` |
| doris-fe | `doris-fe:8030` | `/metrics` |
| doris-be | `doris-be:8040` | `/metrics` |
| node-exporter | `node-exporter:9100` | 默认 |
| prometheus | `localhost:9090` | 默认 |

---

## 贡献指南

### 如何跑测试

```bash
# 1. 启动完整 Docker 环境
bash start-docker.sh

# 2. 生成测试数据 (小批量, 快速验证)
python3 docker/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/gmall_mock_data_generator.py \
  --host localhost -u root -p 123456 \
  --users 100 --spus 20 --orders 500 --seed 42

# 3. 快速冒烟: 只跑一个 ODS + DIM + DWD + DWS 链路
#    (脚本可参考 docker/sql/EcomLakeVoyage/ 下的顺序手动执行)

# 4. 验证数据一致性: ODS 行数 vs DWD 行数 vs DWS 聚合
mysql -h 127.0.0.1 -P 9030 -u root -e "
  SELECT 'dws.trade_user_stats' AS tbl, COUNT(*) AS cnt FROM dws.trade_user_stats
  UNION ALL
  SELECT 'ads.gmv_realtime_dashboard', COUNT(*) FROM ads.gmv_realtime_dashboard;
"

# 5. 查看 Flink 作业状态
#    打开 http://localhost:8081  → Running Jobs 全部应为 RUNNING
```

### 目录新增约定

- **新的 ODS 表**：放 `ods/base/` (CDC 维度) 或 `ods/business/` (CDC 业务) 或 `ods/log/` (Kafka 日志)，必须包含完整 24 项 Flink SET 配置 + Paimon Catalog 创建。
- **新的维度表**：一次性维表放 `dim/batch_base/`，需要 CDC 持续更新的放 `dim/streaming_base/`。
- **新的 DWS 汇总**：Flink SQL 作业 + Doris DDL 两部分，前者写 `dws/business/` 后者对应 `doris_dws_init.sql` 追加。
- **新的 ADS 视图**：直接追加到 `ads/doris_ads_all_in_one.sql`，并在文件顶部注释中登记视图名。

### PR 流程

1. **Fork & Branch**：从 `main` 切分支，命名 `feat/<模块>-<功能>` 或 `fix/<issue-id>-<简述>`。
2. **本地校验**：
   - 至少跑通 `--dry-run` 模式的数据生成；
   - 新增的 Flink SQL 用 `sql-client.sh` 验证语法（能进入 RUNNING / SCHEDULED 状态）；
   - ADS 视图在 Doris 中 `SELECT * LIMIT 10` 不报错。
3. **提交**：`git commit -m "feat(ads): 新增用户复购率分析视图"`，前缀遵循 `feat|fix|docs|refactor|test|chore`。
4. **PR 描述**：贴出关键 SQL 的 `EXPLAIN` 结果或 Grafana 截图（如有变更看板）。
5. **Review**：至少 1 人 approve，CI 通过后合并。

---

## Roadmap

- [x] v1.0.0 — 完成 ODS → DIM → DWD → DWS → ADS 全链路
- [x] v1.0.0 — 4 块 Grafana 预设看板 + Prometheus 采集
- [ ] v1.1.0 — 接入 Iceberg 作为 Paimon 的备选湖格式对比
- [ ] v1.2.0 — Hive 3.1 Metastore 集成 + Flink SQL Hive Dialect
- [ ] v1.2.0 — Flink CEP 实时异常检测 (订单欺诈/刷单识别)
- [ ] v1.3.0 — 增加用户画像标签体系 DWS/ADS 层
- [ ] v1.3.0 — Doris 行级权限 + Grafana 多租户数据源
- [ ] v2.0.0 — K8s (Helm) 部署替代 Docker Compose
- [ ] v2.0.0 — 端到端自动化测试 + 数据质量校验 (Great Expectations)

---

## Changelog

### [1.0.0] - 2026-08-22

**Added**
- 完整电商实时数仓：20 张 ODS 表 + 8 张 DIM 表 + 12 张 DWD 表 + DWS/ADS 全量视图
- 5 大 ADS 分析模型：留存 / 路径 / RFM / GMV 大屏 / 商品趋势异常预警
- 高逼真模拟数据生成器：覆盖 14 张表、外键关联、订单 7 状态流转、优惠券核销逻辑
- Docker Compose 一键部署：Kafka / MySQL / Debezium / Flink / Doris
- Prometheus + Grafana 监控栈：4 块预设可视化看板

---

## License

**MIT License** — 详见 [LICENSE](LICENSE) 文件。

SPDX-License-Identifier: **MIT**

```
MIT License

Copyright (c) 2026 EcomLakeVoyage Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
