-- ============================================================
-- DWD 日志同步 - 曝光日志（Display Log）宽表
-- 流模式，INSERT INTO 持续同步
-- append-only（无主键，无 changelog-producer），dt 分区
--
-- Join 策略：
--   dim.user_info → Lookup Join (处理时间)
--   dim.sku_info  → Lookup Join (处理时间，炸裂后的 sku_id_str CAST 为 BIGINT)
--
-- 特殊处理：
--   item_ids 炸裂——UNNEST(SPLIT(item_ids, ',')) 把逗号分隔的 item_ids 展开成多行
--   item_pos 保留原始字符串（不做一一对应炸裂）
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/log/dwd_log_display.sql
-- ============================================================
-- 前置条件：dim.user_info + dim.sku_info 必须先执行
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
CREATE DATABASE IF NOT EXISTS dwd;

-- ========== Paimon DWD Target（append-only，无主键） ==========
CREATE TABLE IF NOT EXISTS dwd.log_display (
    -- ODS 原有字段
    mid             STRING,
    user_id         BIGINT,
    page_id         STRING,
    display_type    STRING,
    item_ids        STRING,
    item_pos        STRING,
    -- 维度字段（dim.sku_info，由炸裂后的 sku_id 关联得到）
    sku_id          BIGINT,
    sku_name        STRING,
    spu_name        STRING,
    category3_name  STRING,
    category2_name  STRING,
    category1_name  STRING,
    brand_name      STRING,
    -- 维度字段（dim.user_info）
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
    'orc.compression' = 'zstd'
);

-- ========== 同步作业（UNNEST 炸裂 + Lookup Join 拼宽表） ==========
-- item_ids 炸裂：UNNEST(SPLIT(d.item_ids, ',')) 把逗号分隔的 item_ids 展开成多行
-- dim.user_info: Lookup Join（处理时间关联，维度变更频率低）
-- dim.sku_info : Lookup Join（处理时间关联，炸裂后的 sku_id_str CAST 为 BIGINT）
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
    -- 内层：炸裂 item_ids，得到 sku_id + 保留 proctime
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
    -- 炸裂：item_ids 逗号分隔展开成多行
    CROSS JOIN UNNEST(SPLIT(d.item_ids, ',')) AS sku_id_str
) t
-- Lookup Join: 处理时间关联（user_info 维度变更频率低）
LEFT JOIN dim.user_info FOR SYSTEM_TIME AS OF t.proctime AS u
    ON t.user_id = u.id
-- Lookup Join: 处理时间关联（sku_info，炸裂后的 sku_id 已 CAST 为 BIGINT）
LEFT JOIN dim.sku_info FOR SYSTEM_TIME AS OF t.proctime AS s
    ON t.sku_id = s.id;
