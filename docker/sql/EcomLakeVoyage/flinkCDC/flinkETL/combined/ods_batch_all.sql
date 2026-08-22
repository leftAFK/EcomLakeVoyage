-- ============================================================
-- ODS 批量同步合并作业（4 张 JDBC 维度表）
-- 用 Statement Set 合并 4 个 INSERT OVERWRITE 为 1 个 Flink 作业
-- 占用 slot: 1（跑完后自动 FINISHED 释放 slot）
-- 数据源: MySQL JDBC (gmall_base)
-- 目标: Paimon ods.base_brand/base_category/base_dic/base_region
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/combined/ods_batch_all.sql
-- ============================================================

-- ========== 环境配置 ==========
SET 'execution.checkpointing.interval' = '30s';
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
SET 'table.exec.state.ttl' = '0s';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'execution.runtime-mode' = 'batch';

-- ========== Paimon Catalog ==========
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon_warehouse'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ods;

-- ============================================================
-- 1. base_brand（品牌表）
-- ============================================================
CREATE TEMPORARY TABLE mysql_base_brand (
    id          BIGINT,
    brand_name  STRING,
    logo_url    STRING,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql:3306/gmall_base',
    'username' = 'root',
    'password' = '123456',
    'table-name' = 'base_brand',
    'scan.fetch-size' = '200'
);

CREATE TABLE IF NOT EXISTS ods.base_brand (
    id          BIGINT,
    brand_name  STRING,
    logo_url    STRING,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
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
-- ============================================================
-- 2. base_category（分类表）
-- ============================================================
CREATE TEMPORARY TABLE mysql_base_category (
    id            BIGINT,
    category_name STRING,
    `level`       TINYINT,
    parent_id     BIGINT,
    create_time   TIMESTAMP(3),
    update_time   TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql:3306/gmall_base',
    'username' = 'root',
    'password' = '123456',
    'table-name' = 'base_category',
    'scan.fetch-size' = '200'
);

CREATE TABLE IF NOT EXISTS ods.base_category (
    id            BIGINT,
    category_name STRING,
    `level`       TINYINT,
    parent_id     BIGINT,
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
-- ============================================================
-- 3. base_dic（字典表）
-- ============================================================
CREATE TEMPORARY TABLE mysql_base_dic (
    id          BIGINT,
    dic_type    STRING,
    code        STRING,
    name        STRING,
    `sort`      INT,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql:3306/gmall_base',
    'username' = 'root',
    'password' = '123456',
    'table-name' = 'base_dic',
    'scan.fetch-size' = '200'
);

CREATE TABLE IF NOT EXISTS ods.base_dic (
    id          BIGINT,
    dic_type    STRING,
    code        STRING,
    name        STRING,
    `sort`      INT,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
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
-- ============================================================
-- 4. base_region（地区表）
-- ============================================================
CREATE TEMPORARY TABLE mysql_base_region (
    id          BIGINT,
    region_code STRING,
    region_name STRING,
    `level`     TINYINT,
    parent_id   BIGINT,
    big_region  STRING,
    create_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql:3306/gmall_base',
    'username' = 'root',
    'password' = '123456',
    'table-name' = 'base_region',
    'scan.fetch-size' = '200'
);

CREATE TABLE IF NOT EXISTS ods.base_region (
    id          BIGINT,
    region_code STRING,
    region_name STRING,
    `level`     TINYINT,
    parent_id   BIGINT,
    big_region  STRING,
    create_time TIMESTAMP(3),
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
-- ============================================================
-- Statement Set：合并 4 个 INSERT OVERWRITE 为 1 个作业
-- ============================================================
BEGIN STATEMENT SET;

INSERT OVERWRITE ods.base_brand
SELECT id, brand_name, logo_url, create_time, update_time
FROM mysql_base_brand;

INSERT OVERWRITE ods.base_category
SELECT id, category_name, `level`, parent_id, create_time, update_time
FROM mysql_base_category;

INSERT OVERWRITE ods.base_dic
SELECT id, dic_type, code, name, `sort`, create_time, update_time
FROM mysql_base_dic;

INSERT OVERWRITE ods.base_region
SELECT id, region_code, region_name, `level`, parent_id, big_region, create_time
FROM mysql_base_region;

END;
