-- ============================================================
-- ODS Kafka 日志同步合并作业（5 张 Kafka 日志表）
-- 用 Statement Set 合并 5 个 INSERT INTO 为 1 个 Flink 作业
-- 占用 slot: 1（常驻流作业）
-- 数据源: Kafka (ods_log_startup/page_view/action/display/error)
-- 目标: Paimon ods.log_* (append-only，无主键)
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/combined/ods_kafka_all.sql
-- ============================================================

-- ========== 环境配置 ==========
SET 'execution.checkpointing.interval' = '30s';
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
SET 'table.exec.state.ttl' = '0s';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'execution.runtime-mode' = 'streaming';

-- ========== Paimon Catalog ==========
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon_warehouse'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ods;

-- ============================================================
-- 1. 启动日志 startup
-- ============================================================
CREATE TEMPORARY TABLE kafka_log_startup (
    mid           STRING,
    user_id       BIGINT,
    appid         STRING,
    os            STRING,
    area          STRING,
    version       STRING,
    channel       STRING,
    entry         STRING,
    loading_time  INT,
    open_ad_id    STRING,
    open_ad_ms    INT,
    create_time   TIMESTAMP(3),
    proctime AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'ods_log_startup',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink_ods_log_startup',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE IF NOT EXISTS ods.log_startup (
    mid           STRING,
    user_id       BIGINT,
    appid         STRING,
    os            STRING,
    area          STRING,
    version       STRING,
    channel       STRING,
    entry         STRING,
    loading_time  INT,
    open_ad_id    STRING,
    open_ad_ms    INT,
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
-- ============================================================
-- 2. 页面浏览日志 page_view
-- ============================================================
CREATE TEMPORARY TABLE kafka_log_page_view (
    mid            STRING,
    user_id        BIGINT,
    page_id        STRING,
    page_name      STRING,
    last_page_id   STRING,
    jump_count     INT,
    during_time    INT,
    source_type    STRING,
    create_time    TIMESTAMP(3),
    proctime AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'ods_log_page_view',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink_ods_log_page_view',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE IF NOT EXISTS ods.log_page_view (
    mid            STRING,
    user_id        BIGINT,
    page_id        STRING,
    page_name      STRING,
    last_page_id   STRING,
    jump_count     INT,
    during_time    INT,
    source_type    STRING,
    create_time    TIMESTAMP(3),
    dt             STRING
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
-- ============================================================
-- 3. 动作日志 action
-- ============================================================
CREATE TEMPORARY TABLE kafka_log_action (
    mid             STRING,
    user_id         BIGINT,
    item_type       STRING,
    item_id         STRING,
    target_page_id  STRING,
    create_time     TIMESTAMP(3),
    proctime AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'ods_log_action',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink_ods_log_action',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE IF NOT EXISTS ods.log_action (
    mid             STRING,
    user_id         BIGINT,
    item_type       STRING,
    item_id         STRING,
    target_page_id  STRING,
    create_time     TIMESTAMP(3),
    dt              STRING
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
-- ============================================================
-- 4. 曝光日志 display
-- ============================================================
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
-- ============================================================
-- 5. 错误日志 error
-- ============================================================
CREATE TEMPORARY TABLE kafka_log_error (
    mid           STRING,
    user_id       BIGINT,
    appid         STRING,
    err_code      STRING,
    err_name      STRING,
    err_content   STRING,
    create_time   TIMESTAMP(3),
    proctime AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'ods_log_error',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink_ods_log_error',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE IF NOT EXISTS ods.log_error (
    mid           STRING,
    user_id       BIGINT,
    appid         STRING,
    err_code      STRING,
    err_name      STRING,
    err_content   STRING,
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
-- ============================================================
-- Statement Set：合并 5 个 Kafka INSERT 为 1 个作业
-- ============================================================
BEGIN STATEMENT SET;

INSERT INTO ods.log_startup
SELECT
    mid, user_id, appid, os, area, version, channel, entry,
    loading_time, open_ad_id, open_ad_ms, create_time,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt
FROM kafka_log_startup;

INSERT INTO ods.log_page_view
SELECT
    mid, user_id, page_id, page_name, last_page_id,
    jump_count, during_time, source_type, create_time,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt
FROM kafka_log_page_view;

INSERT INTO ods.log_action
SELECT
    mid, user_id, item_type, item_id, target_page_id, create_time,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt
FROM kafka_log_action;

INSERT INTO ods.log_display
SELECT
    mid, user_id, page_id, display_type, item_ids, item_pos, create_time,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt
FROM kafka_log_display;

INSERT INTO ods.log_error
SELECT
    mid, user_id, appid, err_code, err_name, err_content, create_time,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt
FROM kafka_log_error;

END;
