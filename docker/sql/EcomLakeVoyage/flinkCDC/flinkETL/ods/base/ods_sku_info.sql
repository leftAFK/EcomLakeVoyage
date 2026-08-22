-- ODS CDC 实时同步 - sku_info（SKU表）
-- 流模式，INSERT INTO 持续同步
-- changelog-producer=input，产完整 changelog 供下游 temporal join

-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/base/ods_sku_info.sql

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
CREATE TEMPORARY TABLE mysql_sku_info (
    id           BIGINT,
    sku_name     STRING,
    spu_id       BIGINT,
    category3_id BIGINT,
    brand_id     BIGINT,
    price        DECIMAL(18,2),
    weight       DECIMAL(10,2),
    img_url      STRING,
    is_sale      TINYINT,
    sku_attr     STRING,
    create_time  TIMESTAMP(3),
    update_time  TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'gmall_base',
    'table-name' = 'sku_info',
    'server-id' = '5466-5476',
    'server-time-zone' = 'Asia/Shanghai',
    -- 'scan.startup.mode' = 'initial' -- 初始化后使用latest-offset
    'scan.startup.mode' = 'latest-offset'
);

-- ========== Paimon Target ==========
CREATE TABLE IF NOT EXISTS ods.sku_info (
    id           BIGINT,
    sku_name     STRING,
    spu_id       BIGINT,
    category3_id BIGINT,
    brand_id     BIGINT,
    price        DECIMAL(18,2),
    weight       DECIMAL(10,2),
    img_url      STRING,
    is_sale      TINYINT,
    sku_attr     STRING,
    create_time  TIMESTAMP(3),
    update_time  TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
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
INSERT INTO ods.sku_info
SELECT id, sku_name, spu_id, category3_id, brand_id, price, weight,
       img_url, is_sale, sku_attr, create_time, update_time
FROM mysql_sku_info;
