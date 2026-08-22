-- ============================================================
-- DWD 日志同步 - 页面浏览（Page View）宽表
-- 流模式，INSERT INTO 持续同步
-- append-only（无主键，无 changelog-producer），dt 分区
--
-- Join 策略：
--   dim.user_info → Lookup Join (处理时间)
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/log/dwd_log_page_view.sql
-- ============================================================
-- 前置条件：dim.user_info 必须先执行
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
CREATE TABLE IF NOT EXISTS dwd.log_page_view (
    -- ODS 原有字段
    mid             STRING,
    user_id         BIGINT,
    page_id         STRING,
    page_name       STRING,
    last_page_id    STRING,
    jump_count      INT,
    during_time     INT,
    source_type     STRING,
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
    'orc.compression' = 'zstd',
    'num-sorted-run.compaction-trigger' = '3',
    'compaction.max.file-num' = '5',
    'compaction.max-size-amplification-percent' = '50',
    'full-compaction.delta-commits' = '10'
);
-- ========== 同步作业（Lookup Join 拼宽表） ==========
-- dim.user_info: Lookup Join（处理时间关联，维度变更频率低）
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
-- Lookup Join: 处理时间关联（user_info 维度变更频率低）
LEFT JOIN dim.user_info FOR SYSTEM_TIME AS OF o.proctime AS u
    ON o.user_id = u.id;
