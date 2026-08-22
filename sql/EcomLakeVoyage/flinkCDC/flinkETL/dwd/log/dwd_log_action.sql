-- ============================================================
-- DWD 日志同步 - 动作日志（Action Log）宽表
-- 流模式，INSERT INTO 持续同步
-- append-only（无主键，无 changelog-producer），dt 分区
--
-- Join 策略：
--   dim.user_info → Lookup Join (处理时间)
--   dim.sku_info  → Lookup Join (处理时间，当 item_type='sku' 时 CAST(item_id AS BIGINT))
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/log/dwd_log_action.sql
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
CREATE TABLE IF NOT EXISTS dwd.log_action (
    -- ODS 原有字段
    mid             STRING,
    user_id         BIGINT,
    item_type       STRING,
    item_id         STRING,
    target_page_id  STRING,
    -- 维度字段（dim.user_info）
    user_nick_name  STRING,
    user_level      TINYINT,
    age_range       STRING,
    gender          TINYINT,
    -- 维度字段（dim.sku_info，仅 item_type='sku' 时有值）
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
    'orc.compression' = 'zstd'
);

-- ========== 同步作业（Lookup Join 拼宽表） ==========
-- dim.user_info: Lookup Join（处理时间关联，维度变更频率低）
-- dim.sku_info : Lookup Join（处理时间关联，当 item_type='sku' 时 CAST(item_id AS BIGINT) 关联）
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
-- Lookup Join: 处理时间关联（user_info 维度变更频率低）
LEFT JOIN dim.user_info FOR SYSTEM_TIME AS OF o.proctime AS u
    ON o.user_id = u.id
-- Lookup Join: 处理时间关联（sku_info，仅 item_type='sku' 时 item_id 才是有效 SKU id）
LEFT JOIN dim.sku_info FOR SYSTEM_TIME AS OF o.proctime AS s
    ON CAST(o.item_id AS BIGINT) = s.id
    AND o.item_type = 'sku';
