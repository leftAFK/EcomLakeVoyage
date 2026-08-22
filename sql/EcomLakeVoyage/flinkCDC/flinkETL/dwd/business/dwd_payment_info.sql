-- ============================================================
-- DWD 实时同步 - payment_info（支付宽表）
-- 流模式，INSERT INTO 持续同步
-- changelog-producer=input，dt 分区
--
-- Join 策略：
--   dim.user_info → Lookup Join (处理时间，维度变更频率低)
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/business/dwd_payment_info.sql
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

-- ========== Paimon DWD Target（支付宽表） ==========
CREATE TABLE IF NOT EXISTS dwd.payment_info (
    id                BIGINT,
    out_trade_no      STRING,
    order_id          BIGINT,
    user_id           BIGINT,
    user_nick_name    STRING,
    user_level        TINYINT,
    age_range         STRING,
    payment_type      TINYINT,
    trade_no          STRING,
    total_amount      DECIMAL(18,2),
    payment_status    SMALLINT,
    create_time       TIMESTAMP(3),
    callback_time     TIMESTAMP(3),
    callback_content  STRING,
    update_time       TIMESTAMP(3),
    dt                STRING,
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
    'orc.compression' = 'zstd'
);

-- ========== 同步作业（Lookup Join 拼宽表） ==========
-- dim.user_info: Lookup Join（处理时间，维度变更频率低）
INSERT INTO dwd.payment_info
SELECT
    p.id,
    p.out_trade_no,
    p.order_id,
    p.user_id,
    u.nick_name         AS user_nick_name,
    u.user_level,
    u.age_range,
    p.payment_type,
    p.trade_no,
    p.total_amount,
    p.payment_status,
    p.create_time,
    p.callback_time,
    p.callback_content,
    p.update_time,
    p.dt
FROM (
    SELECT *, PROCTIME() AS proctime
    FROM ods.payment_info
) p
-- Lookup Join: 处理时间关联（user_info 维度变更频率低）
LEFT JOIN dim.user_info FOR SYSTEM_TIME AS OF p.proctime AS u
    ON p.user_id = u.id;
