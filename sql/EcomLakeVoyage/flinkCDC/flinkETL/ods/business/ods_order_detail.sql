-- ============================================================
-- ODS CDC 实时同步 - order_detail（订单明细表）
-- 流模式，INSERT INTO 持续同步
-- changelog-producer=input，dt 分区，PK=(id, dt)
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/business/ods_order_detail.sql


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
CREATE TEMPORARY TABLE mysql_order_detail (
    id                    BIGINT,
    order_id              BIGINT,
    order_line_no         INT,
    sku_id                BIGINT,
    sku_name              STRING,
    img_url               STRING,
    order_price           DECIMAL(18,2),
    sku_num               INT,
    create_time           TIMESTAMP(3),
    source_type           SMALLINT,
    source_id             BIGINT,
    split_activity_amount DECIMAL(18,4),
    coupon_id             BIGINT,
    split_coupon_amount   DECIMAL(18,4),
    split_total_amount    DECIMAL(18,4),
    update_time           TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'gmall_business',
    'table-name' = 'order_detail',
    'server-id' = '5510-5520',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'initial'
);

-- ========== Paimon Target ==========
CREATE TABLE IF NOT EXISTS ods.order_detail (
    id                    BIGINT,
    order_id              BIGINT,
    order_line_no         INT,
    sku_id                BIGINT,
    sku_name              STRING,
    img_url               STRING,
    order_price           DECIMAL(18,2),
    sku_num               INT,
    create_time           TIMESTAMP(3),
    source_type           SMALLINT,
    source_id             BIGINT,
    split_activity_amount DECIMAL(18,4),
    coupon_id             BIGINT,
    split_coupon_amount   DECIMAL(18,4),
    split_total_amount    DECIMAL(18,4),
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
    'orc.compression' = 'zstd'
);

-- ========== 同步作业 ==========
INSERT INTO ods.order_detail
SELECT id, order_id, order_line_no, sku_id, sku_name, img_url,
       order_price, sku_num, create_time, source_type, source_id,
       split_activity_amount, coupon_id, split_coupon_amount, split_total_amount,
       update_time,
       DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt
FROM mysql_order_detail;
