-- ============================================================
-- DWD 实时同步 - coupon_use（优惠券领用宽表）
-- 流模式，INSERT INTO 持续同步
-- changelog-producer=input，dt 分区
-- dt 基于 get_time（该表无 create_time 列）
--
-- Join 策略：
--   dim.user_info   → Lookup Join (处理时间，维度变更频率低)
--   dim.coupon_info → Lookup Join (处理时间，changelog-producer=none)
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/business/dwd_coupon_use.sql
-- ============================================================
-- 前置条件：dim.user_info + dim.coupon_info 必须先执行
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

-- ========== Paimon DWD Target（优惠券领用宽表） ==========
CREATE TABLE IF NOT EXISTS dwd.coupon_use (
    id                   BIGINT,
    coupon_id            BIGINT,
    coupon_name          STRING,
    coupon_type          TINYINT,
    coupon_reduce_amount DECIMAL(18,2),
    full_amount          DECIMAL(18,2),
    reduce_amount        DECIMAL(18,2),
    use_condition        STRING,
    user_id              BIGINT,
    user_nick_name       STRING,
    user_level           TINYINT,
    age_range            STRING,
    order_id             BIGINT,
    coupon_status        SMALLINT,
    get_time             TIMESTAMP(3),
    lock_time            TIMESTAMP(3),
    using_time           TIMESTAMP(3),
    used_time            TIMESTAMP(3),
    expire_time          TIMESTAMP(3),
    update_time          TIMESTAMP(3),
    dt                   STRING,
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
-- dim.coupon_info: Lookup Join（处理时间，changelog-producer=none）
INSERT INTO dwd.coupon_use
SELECT
    cu.id,
    cu.coupon_id,
    cou.coupon_name,
    cu.coupon_type,
    cu.coupon_reduce_amount,
    cou.full_amount,
    cou.reduce_amount,
    cou.use_condition,
    cu.user_id,
    u.nick_name         AS user_nick_name,
    u.user_level,
    u.age_range,
    cu.order_id,
    cu.coupon_status,
    cu.get_time,
    cu.lock_time,
    cu.using_time,
    cu.used_time,
    cu.expire_time,
    cu.update_time,
    cu.dt
FROM (
    SELECT *, PROCTIME() AS proctime
    FROM ods.coupon_use
) cu
-- Lookup Join: 处理时间关联（user_info 维度变更频率低）
LEFT JOIN dim.user_info FOR SYSTEM_TIME AS OF cu.proctime AS u
    ON cu.user_id = u.id
-- Lookup Join: 处理时间关联（coupon_info 无 changelog）
LEFT JOIN dim.coupon_info FOR SYSTEM_TIME AS OF cu.proctime AS cou
    ON cu.coupon_id = cou.id;
