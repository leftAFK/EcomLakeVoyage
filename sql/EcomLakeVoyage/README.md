# EcomLakeVoyage

基于 Flink + Paimon + Doris 的电商实时数仓

## 架构

MySQL CDC ──→ Paimon ODS ──→ Paimon DWD ──→ Flink 聚合 ──→ Doris DWS ──→ Doris ADS SDK ──→ Kafka ──→ Paimon ODS ──→ Paimon DWD ──→ Flink 聚合 ──→ Doris DWS ──→ Doris ADS ↕ Doris MV (Paimon Catalog)
## 目录结构
sql/EcomLakeVoyage/flinkCDC/flinkETL/ ├── ods/ # ODS 层（MySQL CDC + Kafka 日志同步到 Paimon） │ ├── base/ # 基础维度表 CDC 同步 │ ├── business/ # 业务表 CDC 同步 │ └── log/ # 日志 Kafka 同步（append-only） ├── dim/ # DIM 层（维度宽表，Lookup Join 拼宽） ├── dwd/ # DWD 层（事实宽表，Lookup Join 拼维度） │ └── log/ # 日志 DWD（含曝光日志炸裂） ├── dws/ # DWS 层（Flink 聚合写 Doris + Doris 物化视图） │ └── log/ # 日志 DWS └── doris_*.sql # Doris DDL（DWS 表 + ADS 视图 + 物化视图）

## 技术栈

- **Flink 1.20**：流式 ETL 引擎
- **Paimon 1.0**：湖存储（ODS/DIM/DWD）
- **Doris 2.1.7**：OLAP 查询引擎（DWS/ADS）
- **Kafka 3.9**：日志数据消息队列
- **MySQL 8.0**：业务数据库 + CDC 源
- **Docker**：容器化部署
