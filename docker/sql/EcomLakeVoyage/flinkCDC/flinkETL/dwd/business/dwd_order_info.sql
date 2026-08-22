-- ============================================================
-- DWD 实时同步 - order_info（订单宽表）
-- 流模式，INSERT INTO 持续同步
-- changelog-producer=input，dt 分区
--
-- Join 策略：
--   dim.user_info   → Lookup Join (处理时间，维度变更频率低)
--   dim.base_region → Lookup Join (处理时间，batch 维表无 changelog)
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/business/dwd_order_info.sql
-- ============================================================
-- 前置条件：dim.user_info + dim.base_region 必须先执行


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

-- ========== Paimon DWD Target（订单宽表） ==========
CREATE TABLE IF NOT EXISTS dwd.order_info (
    id                    BIGINT,
    consignee             STRING,
    consignee_tel         STRING,
    total_amount          DECIMAL(18,2),
    order_status          SMALLINT,
    user_id               BIGINT,
    user_nick_name        STRING,
    user_level            TINYINT,
    age_range             STRING,
    gender                TINYINT,
    payment_way           TINYINT,
    delivery_address      STRING,
    order_comment         STRING,
    out_trade_no          STRING,
    trade_body            STRING,
    create_time           TIMESTAMP(3),
    operate_time          TIMESTAMP(3),
    receive_time          TIMESTAMP(3),
    expire_time           TIMESTAMP(3),
    province_id           INT,
    region_name           STRING,
    big_region            STRING,
    coupon_reduce_amount  DECIMAL(18,2),
    original_total_amount DECIMAL(18,2),
    update_time           TIMESTAMP(3),
    dt                    STRING,
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
-- dim.user_info: Lookup Join（处理时间，维度变更频率低，无需版本回溯）
-- dim.base_region: Lookup Join（处理时间，batch 维表）
INSERT INTO dwd.order_info
SELECT
    o.id,
    o.consignee,
    o.consignee_tel,
    o.total_amount,
    o.order_status,
    o.user_id,
    u.nick_name         AS user_nick_name,
    u.user_level,
    u.age_range,
    u.gender,
    o.payment_way,
    o.delivery_address,
    o.order_comment,
    o.out_trade_no,
    o.trade_body,
    o.create_time,
    o.operate_time,
    o.receive_time,
    o.expire_time,
    o.province_id,
    r.region_name,
    r.big_region,
    o.coupon_reduce_amount,
    o.original_total_amount,
    o.update_time,
    o.dt
FROM (
    SELECT *,
           CAST(province_id AS BIGINT) AS region_id,
           PROCTIME() AS proctime
    FROM ods.order_info
) o
-- Lookup Join: 处理时间关联（user_info 维度变更频率低）
LEFT JOIN dim.user_info FOR SYSTEM_TIME AS OF o.proctime AS u
    ON o.user_id = u.id
-- Lookup Join: 处理时间关联（base_region 是 batch 表）
LEFT JOIN dim.base_region FOR SYSTEM_TIME AS OF o.proctime AS r
    ON o.region_id = r.id;
