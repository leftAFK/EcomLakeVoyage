-- ============================================================
-- ODS 日志同步 - 曝光日志（Display Log）
-- 流模式，Kafka JSON → Paimon append-only（无主键，无 changelog-producer）
-- dt 分区，基于 create_time
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/log/ods_log_display.sql
-- ============================================================

-- ========== 环境配置 ==========
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '10min';
SET 'execution.checkpointing.min-pause' = '30s';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '3';
SET 'state.backend' = 'hashmap';
SET 'state.checkpoints.dir' = 'file:///opt/flink/paimon_warehouse/flink-checkpoints';
SET 'state.savepoints.dir' = 'file:///opt/flink/paimon_warehouse/flink-savepoints';
SET 'restart-strategy' = 'failure-rate';
SET 'restart-strategy.failure-rate.max-failures-per-interval' = '3';
SET 'restart-strategy.failure-rate.failure-rate-interval' = '10min';
SET 'restart-strategy.failure-rate.delay' = '30s';
SET 'parallelism.default' = '1';
SET 'table.exec.state.ttl' = '24h';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'execution.runtime-mode' = 'streaming';

-- ========== Paimon Catalog ==========
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon_warehouse'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ods;

-- ========== Kafka Source ==========
CREATE TEMPORARY TABLE kafka_log_display (
    mid           STRING,
    user_id       BIGINT,
    page_id       STRING,
    display_type  STRING,
    item_ids      STRING,
    item_pos      STRING,
    create_time   TIMESTAMP(3),
    proctime AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'ods_log_display',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink_ods_log_display',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- ========== Paimon ODS Target（append-only，无主键） ==========
CREATE TABLE IF NOT EXISTS ods.log_display (
    mid           STRING,
    user_id       BIGINT,
    page_id       STRING,
    display_type  STRING,
    item_ids      STRING,
    item_pos      STRING,
    create_time   TIMESTAMP(3),
    dt            STRING
) PARTITIONED BY (dt) WITH (
    'bucket' = '4',
    'bucket-key' = 'mid',
    'target-file-size' = '128mb',
    'snapshot.time-retained' = '7d',
    'snapshot.num-retained.min' = '10',
    'snapshot.num-retained.max' = '20',
    'file.format' = 'orc',
    'orc.compression' = 'zstd',
    'num-sorted-run.compaction-trigger' = '3',
    'compaction.max.file-num' = '5',
    'compaction.max-size-amplification-percent' = '50',
    'full-compaction.delta-commits' = '10'
);
-- ========== 同步作业 ==========
INSERT INTO ods.log_display
SELECT
    mid,
    user_id,
    page_id,
    display_type,
    item_ids,
    item_pos,
    create_time,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt
FROM kafka_log_display;
