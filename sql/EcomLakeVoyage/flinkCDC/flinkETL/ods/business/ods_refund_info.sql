-- ============================================================
-- ODS CDC 实时同步 - refund_info（退款表）
-- 流模式，INSERT INTO 持续同步
-- changelog-producer=input，dt 分区，PK=(id, dt)
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/business/ods_refund_info.sql

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
CREATE TEMPORARY TABLE mysql_refund_info (
    id                  BIGINT,
    user_id             BIGINT,
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
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'gmall_business',
    'table-name' = 'refund_info',
    'server-id' = '5543-5553',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'initial'
);

-- ========== Paimon Target ==========
CREATE TABLE IF NOT EXISTS ods.refund_info (
    id                  BIGINT,
    user_id             BIGINT,
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
    WATERMARK FOR create_time AS create_time - INTERVAL '5' SECOND,
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
INSERT INTO ods.refund_info
SELECT id, user_id, order_id, order_detail_id, sku_name,
       refund_amount, refund_num, refund_status, refund_type,
       refund_reason, refund_reason_type,
       create_time, refund_time, operate_time, update_time,
       DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt
FROM mysql_refund_info;
