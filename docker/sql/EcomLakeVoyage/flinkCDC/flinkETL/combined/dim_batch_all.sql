-- ============================================================
-- DIM 批量同步合并作业（4 张维度表）
-- 用 Statement Set 合并 4 个 INSERT OVERWRITE 为 1 个 Flink 作业
-- 占用 slot: 1（跑完后自动 FINISHED 释放 slot）
-- 数据源: Paimon ods.* (batch scan)
-- 目标: Paimon dim.* (changelog-producer=none，下游 lookup join)
-- 依赖: 必须先跑完 ods_batch_all.sql
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/combined/dim_batch_all.sql
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
SET 'table.exec.state.ttl' = '1h';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'execution.runtime-mode' = 'batch';

-- ========== Paimon Catalog ==========
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon_warehouse'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS dim;

-- ============================================================
-- 1. dim.base_brand（品牌维度表）
-- ============================================================
CREATE TABLE IF NOT EXISTS dim.base_brand (
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
-- 2. dim.base_category（分类维度表，自连接拼接三级分类）
-- ============================================================
CREATE TABLE IF NOT EXISTS dim.base_category (
    id               BIGINT,
    category_name    STRING,
    `level`          TINYINT,
    parent_id        BIGINT,
    category3_id     BIGINT,
    category3_name   STRING,
    category2_id     BIGINT,
    category2_name   STRING,
    category1_id     BIGINT,
    category1_name   STRING,
    create_time      TIMESTAMP(3),
    update_time      TIMESTAMP(3),
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
-- 3. dim.base_dic（字典维度表）
-- ============================================================
CREATE TABLE IF NOT EXISTS dim.base_dic (
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
-- 4. dim.base_region（地区维度表）
-- ============================================================
CREATE TABLE IF NOT EXISTS dim.base_region (
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

INSERT OVERWRITE dim.base_brand
SELECT id, brand_name, logo_url, create_time, update_time
FROM ods.base_brand;

INSERT OVERWRITE dim.base_category
SELECT
    c3.id,
    c3.category_name,
    c3.`level`,
    c3.parent_id,
    c3.id             AS category3_id,
    c3.category_name  AS category3_name,
    c2.id             AS category2_id,
    c2.category_name  AS category2_name,
    c1.id             AS category1_id,
    c1.category_name  AS category1_name,
    c3.create_time,
    c3.update_time
FROM ods.base_category c3
LEFT JOIN ods.base_category c2 ON c3.parent_id = c2.id
LEFT JOIN ods.base_category c1 ON c2.parent_id = c1.id;

INSERT OVERWRITE dim.base_dic
SELECT id, dic_type, code, name, `sort`, create_time, update_time
FROM ods.base_dic;

INSERT OVERWRITE dim.base_region
SELECT id, region_code, region_name, `level`, parent_id, big_region, create_time
FROM ods.base_region;

END;
