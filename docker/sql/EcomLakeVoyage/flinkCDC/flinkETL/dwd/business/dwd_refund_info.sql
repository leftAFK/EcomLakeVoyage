-- ============================================================
-- DWD 实时同步 - refund_info（退款宽表）
-- 流模式，INSERT INTO 持续同步
-- changelog-producer=input，dt 分区
--
-- Join 策略：
--   dim.user_info → Lookup Join (处理时间，维度变更频率低)
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/business/dwd_refund_info.sql
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
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'execution.runtime-mode' = 'streaming';

-- ========== Paimon Catalog ==========
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon_warehouse'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS dwd;

-- ========== Paimon DWD Target（退款宽表） ==========
CREATE TABLE IF NOT EXISTS dwd.refund_info (
    id                  BIGINT,
    user_id             BIGINT,
    user_nick_name      STRING,
    user_level          TINYINT,
    age_range           STRING,
    order_id            BIGINT,
    order_detail_id     BIGINT,
    sku_name            STRING,
    refund_amount       DECIMAL(18,2),
    refund_num          INT,
    refund_status       SMALLINT,
    refund_type         TINYINT,
    refund_reason       STRING,
    refund_reason_type  TINYINT,
    create_time         TIMESTAMP(3),
    refund_time         TIMESTAMP(3),
    operate_time        TIMESTAMP(3),
    update_time         TIMESTAMP(3),
    dt                  STRING,
    PRIMARY KEY (id, dt) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
    'changelog-producer' = 'input',
    'merge-engine' = 'deduplicate',
    'bucket' = '4',
    'target-file-size' = '128mb',
    'snapshot.time-retained' = '7d',
    'snapshot.num-retained.min' = '10',
    'snapshot.num-retained.max' = '20',
    'changelog.time-retained' = '7d',
    'file.format' = 'orc',
    'orc.compression' = 'zstd',
    'num-sorted-run.compaction-trigger' = '3',
    'compaction.max.file-num' = '5',
    'compaction.max-size-amplification-percent' = '50',
    'full-compaction.delta-commits' = '10'
);
-- ========== 同步作业（Lookup Join 拼宽表） ==========
-- dim.user_info: Lookup Join（处理时间，维度变更频率低）
INSERT INTO dwd.refund_info
SELECT
    r.id,
    r.user_id,
    u.nick_name         AS user_nick_name,
    u.user_level,
    u.age_range,
    r.order_id,
    r.order_detail_id,
    r.sku_name,
    r.refund_amount,
    r.refund_num,
    r.refund_status,
    r.refund_type,
    r.refund_reason,
    r.refund_reason_type,
    r.create_time,
    r.refund_time,
    r.operate_time,
    r.update_time,
    r.dt
FROM (
    SELECT *, PROCTIME() AS proctime
    FROM ods.refund_info
) r
-- Lookup Join: 处理时间关联（user_info 维度变更频率低）
LEFT JOIN dim.user_info FOR SYSTEM_TIME AS OF r.proctime AS u
    ON r.user_id = u.id;
