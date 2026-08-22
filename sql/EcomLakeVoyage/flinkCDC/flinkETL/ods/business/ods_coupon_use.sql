-- ============================================================
-- ODS CDC 实时同步 - coupon_use（优惠券领用核销表）
-- 流模式，INSERT INTO 持续同步
-- changelog-producer=input，dt 分区，PK=(id, dt)
-- dt 基于 get_time（该表无 create_time 列）

-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/business/ods_coupon_use.sql


-- ============================================================
-- ODS CDC 实时同步 - coupon_use（优惠券领用核销表）
-- 流模式，INSERT INTO 持续同步
-- changelog-producer=input，dt 分区，PK=(id, dt)
-- dt 基于 get_time（该表无 create_time 列）
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/business/ods_coupon_use.sql
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
CREATE DATABASE IF NOT EXISTS ods;

-- ========== MySQL CDC Source ==========
CREATE TEMPORARY TABLE mysql_coupon_use (
    id                  BIGINT,
    coupon_id           BIGINT,
    coupon_type         TINYINT,
    user_id             BIGINT,
    order_id            BIGINT,
    coupon_status       SMALLINT,
    coupon_reduce_amount DECIMAL(18,2),
    get_time            TIMESTAMP(3),
    lock_time           TIMESTAMP(3),
    using_time          TIMESTAMP(3),
    used_time           TIMESTAMP(3),
    expire_time         TIMESTAMP(3),
    update_time         TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'gmall_business',
    'table-name' = 'coupon_use',
    'server-id' = '5554-5564',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'initial'
);

-- ========== Paimon Target ==========
CREATE TABLE IF NOT EXISTS ods.coupon_use (
    id                  BIGINT,
    coupon_id           BIGINT,
    coupon_type         TINYINT,
    user_id             BIGINT,
    order_id            BIGINT,
    coupon_status       SMALLINT,
    coupon_reduce_amount DECIMAL(18,2),
    get_time            TIMESTAMP(3),
    lock_time           TIMESTAMP(3),
    using_time          TIMESTAMP(3),
    used_time           TIMESTAMP(3),
    expire_time         TIMESTAMP(3),
    update_time          TIMESTAMP(3),
    dt                   STRING,
    WATERMARK FOR get_time AS get_time - INTERVAL '5' SECOND,
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

-- ========== 同步作业 ==========
INSERT INTO ods.coupon_use
SELECT id, coupon_id, coupon_type, user_id, order_id, coupon_status,
       coupon_reduce_amount, get_time, lock_time, using_time,
       used_time, expire_time, update_time,
       DATE_FORMAT(get_time, 'yyyy-MM-dd') AS dt
FROM mysql_coupon_use;
