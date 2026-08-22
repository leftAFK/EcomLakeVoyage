-- ============================================================
-- ODS CDC 实时同步 - order_info（订单主表）
-- 流模式，INSERT INTO 持续同步
-- changelog-producer=input，dt 分区，PK=(id, dt)
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/business/ods_order_info.sql


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
CREATE TEMPORARY TABLE mysql_order_info (
    id                    BIGINT,
    consignee             STRING,
    consignee_tel         STRING,
    total_amount          DECIMAL(18,2),
    order_status          SMALLINT,
    user_id               BIGINT,
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
    coupon_reduce_amount  DECIMAL(18,2),
    original_total_amount DECIMAL(18,2),
    update_time           TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'gmall_business',
    'table-name' = 'order_info',
    'server-id' = '5488-5498',
    'server-time-zone' = 'Asia/Shanghai',
    -- 'scan.startup.mode' = 'initial'
    'scan.startup.mode' = 'latest-offset'
);

-- ========== Paimon Target ==========
CREATE TABLE IF NOT EXISTS ods.order_info (
    id                    BIGINT,
    consignee             STRING,
    consignee_tel         STRING,
    total_amount          DECIMAL(18,2),
    order_status          SMALLINT,
    user_id               BIGINT,
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
    coupon_reduce_amount  DECIMAL(18,2),
    original_total_amount DECIMAL(18,2),
    update_time           TIMESTAMP(3),
    dt                    STRING,
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
    'orc.compression' = 'zstd',
    'num-sorted-run.compaction-trigger' = '3',
    'compaction.max.file-num' = '5',
    'compaction.max-size-amplification-percent' = '50',
    'full-compaction.delta-commits' = '10'
);
-- ========== 同步作业 ==========
INSERT INTO ods.order_info
SELECT id, consignee, consignee_tel, total_amount, order_status, user_id,
       payment_way, delivery_address, order_comment, out_trade_no, trade_body,
       create_time, operate_time, receive_time, expire_time, province_id,
       coupon_reduce_amount, original_total_amount, update_time,
       DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt
FROM mysql_order_info;
