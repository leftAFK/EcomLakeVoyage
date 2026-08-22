-- ============================================================
-- DWD 日志宽表合并作业（5 张日志宽表）
-- 用 Statement Set 合并 5 个 INSERT INTO 为 1 个 Flink 作业
-- 占用 slot: 1（常驻流作业）
-- 数据源: Paimon ods.log_* (5 张)
-- 维表: Paimon dim.user_info + dim.sku_info
-- 目标: Paimon dwd.log_* (append-only，无主键)
-- 依赖: 必须先跑完 ods_kafka_all.sql + dim_streaming_all.sql
--   - log_startup   依赖 ods.log_startup + dim.user_info
--   - log_page_view 依赖 ods.log_page_view + dim.user_info
--   - log_action    依赖 ods.log_action + dim.user_info + dim.sku_info
--   - log_display   依赖 ods.log_display + dim.user_info + dim.sku_info (UNNEST 炸裂)
--   - log_error     依赖 ods.log_error + dim.user_info
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/combined/dwd_log_all.sql
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
SET 'table.exec.state.ttl' = '1h';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'execution.runtime-mode' = 'streaming';

-- ========== Paimon Catalog ==========
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon_warehouse'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS dwd;

-- ============================================================
-- 1. dwd.log_startup（启动日志宽表，拼 user）
-- ============================================================
CREATE TABLE IF NOT EXISTS dwd.log_startup (
    mid             STRING,
    user_id         BIGINT,
    appid           STRING,
    os              STRING,
    area            STRING,
    version         STRING,
    channel         STRING,
    entry           STRING,
    loading_time    INT,
    open_ad_id      STRING,
    open_ad_ms      INT,
    user_nick_name  STRING,
    user_level      TINYINT,
    age_range       STRING,
    gender          TINYINT,
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
-- 2. dwd.log_page_view（页面浏览宽表，拼 user）
-- ============================================================
CREATE TABLE IF NOT EXISTS dwd.log_page_view (
    mid             STRING,
    user_id         BIGINT,
    page_id         STRING,
    page_name       STRING,
    last_page_id    STRING,
    jump_count      INT,
    during_time     INT,
    source_type     STRING,
    user_nick_name  STRING,
    user_level      TINYINT,
    age_range       STRING,
    gender          TINYINT,
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
-- 3. dwd.log_action（动作日志宽表，拼 user + sku）
-- ============================================================
CREATE TABLE IF NOT EXISTS dwd.log_action (
    mid             STRING,
    user_id         BIGINT,
    item_type       STRING,
    item_id         STRING,
    target_page_id  STRING,
    user_nick_name  STRING,
    user_level      TINYINT,
    age_range       STRING,
    gender          TINYINT,
    sku_name        STRING,
    spu_name        STRING,
    category3_name  STRING,
    category2_name  STRING,
    category1_name  STRING,
    brand_name      STRING,
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
-- 4. dwd.log_display（曝光日志宽表，UNNEST 炸裂 item_ids + 拼 user + sku）
-- ============================================================
CREATE TABLE IF NOT EXISTS dwd.log_display (
    mid             STRING,
    user_id         BIGINT,
    page_id         STRING,
    display_type    STRING,
    item_ids        STRING,
    item_pos        STRING,
    sku_id          BIGINT,
    sku_name        STRING,
    spu_name        STRING,
    category3_name  STRING,
    category2_name  STRING,
    category1_name  STRING,
    brand_name      STRING,
    user_nick_name  STRING,
    user_level      TINYINT,
    age_range       STRING,
    gender          TINYINT,
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
-- 5. dwd.log_error（错误日志宽表，拼 user）
-- ============================================================
CREATE TABLE IF NOT EXISTS dwd.log_error (
    mid             STRING,
    user_id         BIGINT,
    appid           STRING,
    err_code        STRING,
    err_name        STRING,
    err_content     STRING,
    user_nick_name  STRING,
    user_level      TINYINT,
    age_range       STRING,
    gender          TINYINT,
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
-- Statement Set：合并 5 个 INSERT INTO 为 1 个作业
-- ============================================================
BEGIN STATEMENT SET;

-- 1. log_startup
INSERT INTO dwd.log_startup
SELECT
    o.mid,
    o.user_id,
    o.appid,
    o.os,
    o.area,
    o.version,
    o.channel,
    o.entry,
    o.loading_time,
    o.open_ad_id,
    o.open_ad_ms,
    u.nick_name     AS user_nick_name,
    u.user_level,
    u.age_range,
    u.gender,
    o.create_time,
    o.dt
FROM (
    SELECT *, PROCTIME() AS proctime
    FROM ods.log_startup
) o
LEFT JOIN dim.user_info FOR SYSTEM_TIME AS OF o.proctime AS u
    ON o.user_id = u.id;

-- 2. log_page_view
INSERT INTO dwd.log_page_view
SELECT
    o.mid,
    o.user_id,
    o.page_id,
    o.page_name,
    o.last_page_id,
    o.jump_count,
    o.during_time,
    o.source_type,
    u.nick_name     AS user_nick_name,
    u.user_level,
    u.age_range,
    u.gender,
    o.create_time,
    o.dt
FROM (
    SELECT *, PROCTIME() AS proctime
    FROM ods.log_page_view
) o
LEFT JOIN dim.user_info FOR SYSTEM_TIME AS OF o.proctime AS u
    ON o.user_id = u.id;

-- 3. log_action
INSERT INTO dwd.log_action
SELECT
    o.mid,
    o.user_id,
    o.item_type,
    o.item_id,
    o.target_page_id,
    u.nick_name         AS user_nick_name,
    u.user_level,
    u.age_range,
    u.gender,
    s.sku_name,
    s.spu_name,
    s.category3_name,
    s.category2_name,
    s.category1_name,
    s.brand_name,
    o.create_time,
    o.dt
FROM (
    SELECT *, PROCTIME() AS proctime
    FROM ods.log_action
) o
LEFT JOIN dim.user_info FOR SYSTEM_TIME AS OF o.proctime AS u
    ON o.user_id = u.id
LEFT JOIN dim.sku_info FOR SYSTEM_TIME AS OF o.proctime AS s
    ON CAST(o.item_id AS BIGINT) = s.id
    AND o.item_type = 'sku';

-- 4. log_display (UNNEST 炸裂 item_ids)
INSERT INTO dwd.log_display
SELECT
    t.mid,
    t.user_id,
    t.page_id,
    t.display_type,
    t.item_ids,
    t.item_pos,
    t.sku_id,
    s.sku_name,
    s.spu_name,
    s.category3_name,
    s.category2_name,
    s.category1_name,
    s.brand_name,
    u.nick_name         AS user_nick_name,
    u.user_level,
    u.age_range,
    u.gender,
    t.create_time,
    t.dt
FROM (
    SELECT
        d.mid,
        d.user_id,
        d.page_id,
        d.display_type,
        d.item_ids,
        d.item_pos,
        CAST(sku_id_str AS BIGINT) AS sku_id,
        d.create_time,
        d.dt,
        d.proctime
    FROM (
        SELECT *, PROCTIME() AS proctime
        FROM ods.log_display
    ) d
    CROSS JOIN UNNEST(SPLIT(d.item_ids, ',')) AS sku_id_str
) t
LEFT JOIN dim.user_info FOR SYSTEM_TIME AS OF t.proctime AS u
    ON t.user_id = u.id
LEFT JOIN dim.sku_info FOR SYSTEM_TIME AS OF t.proctime AS s
    ON t.sku_id = s.id;

-- 5. log_error
INSERT INTO dwd.log_error
SELECT
    o.mid,
    o.user_id,
    o.appid,
    o.err_code,
    o.err_name,
    o.err_content,
    u.nick_name     AS user_nick_name,
    u.user_level,
    u.age_range,
    u.gender,
    o.create_time,
    o.dt
FROM (
    SELECT *, PROCTIME() AS proctime
    FROM ods.log_error
) o
LEFT JOIN dim.user_info FOR SYSTEM_TIME AS OF o.proctime AS u
    ON o.user_id = u.id;

END;
