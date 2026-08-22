-- ODS CDC 实时同步 - coupon_info（优惠券模板表）
-- 流模式，INSERT INTO 持续同步
-- changelog-producer=none，下游 lookup join 只需最新态
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/base/ods_coupon_info.sql
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

-- ========== MySQL CDC Source ==========
CREATE TEMPORARY TABLE mysql_coupon_info (
    id            BIGINT,
    coupon_type   TINYINT,
    full_amount   DECIMAL(18,2),
    reduce_amount DECIMAL(18,2),
    coupon_name   STRING,
    use_condition STRING,
    create_time   TIMESTAMP(3),
    update_time   TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'gmall_base',
    'table-name' = 'coupon_info',
    'server-id' = '5444-5454',
    'server-time-zone' = 'Asia/Shanghai',
    -- 'scan.startup.mode' = 'initial' 初始化后使用latest-offset
    'scan.startup.mode' = 'latest-offset'
);

-- ========== Paimon Target ==========
CREATE TABLE IF NOT EXISTS ods.coupon_info (
    id            BIGINT,
    coupon_type   TINYINT,
    full_amount   DECIMAL(18,2),
    reduce_amount DECIMAL(18,2),
    coupon_name   STRING,
    use_condition STRING,
    create_time   TIMESTAMP(3),
    update_time   TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'changelog-producer' = 'none',
    'merge-engine' = 'deduplicate',
    'bucket' = '1',
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
INSERT INTO ods.coupon_info
SELECT id, coupon_type, full_amount, reduce_amount, coupon_name,
       use_condition, create_time, update_time
FROM mysql_coupon_info;
