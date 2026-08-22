-- ============================================================
-- DIM 实时同步 - sku_info（SKU 维度宽表）
-- 流模式，INSERT INTO 持续同步
-- changelog-producer=input，产完整 changelog 供下游 temporal join
--
-- 关联关系：
--   ods.sku_info.spu_id       → ods.spu_info.id       (Lookup Join → spu_name)
--   ods.sku_info.brand_id     → ods.base_brand.id     (Lookup Join → brand_name)
--   ods.sku_info.category3_id → dim.base_category.id  (Lookup Join → 展平分类层级)
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dim/streaming_base/dim_sku_info.sql
-- ============================================================
-- 前置条件：dim.base_category 必须先执行（batch 模式）
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
CREATE DATABASE IF NOT EXISTS dim;

-- ========== Paimon DIM Target（SKU 维度宽表） ==========
CREATE TABLE IF NOT EXISTS dim.sku_info (
    id               BIGINT,
    sku_name         STRING,
    spu_id           BIGINT,
    spu_name         STRING,
    category3_id     BIGINT,
    category3_name   STRING,
    category2_id     BIGINT,
    category2_name   STRING,
    category1_id     BIGINT,
    category1_name   STRING,
    brand_id         BIGINT,
    brand_name       STRING,
    price            DECIMAL(18,2),
    weight           DECIMAL(10,2),
    img_url          STRING,
    is_sale          TINYINT,
    sku_attr         STRING,
    create_time      TIMESTAMP(3),
    update_time      TIMESTAMP(3),
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
-- ========== 同步作业（Lookup Join 拼宽表） ==========
-- 读取 ods.sku_info 的 changelog 流，通过 lookup join 拼接维度信息
INSERT INTO dim.sku_info
SELECT
    s.id,
    s.sku_name,
    s.spu_id,
    spu.spu_name,
    s.category3_id,
    cat.category3_name,
    cat.category2_id,
    cat.category2_name,
    cat.category1_id,
    cat.category1_name,
    s.brand_id,
    brand.brand_name,
    s.price,
    s.weight,
    s.img_url,
    s.is_sale,
    s.sku_attr,
    s.create_time,
    s.update_time
FROM (
    SELECT *, PROCTIME() AS proctime
    FROM ods.sku_info
) s
LEFT JOIN ods.spu_info FOR SYSTEM_TIME AS OF s.proctime AS spu
    ON s.spu_id = spu.id
LEFT JOIN ods.base_brand FOR SYSTEM_TIME AS OF s.proctime AS brand
    ON s.brand_id = brand.id
LEFT JOIN dim.base_category FOR SYSTEM_TIME AS OF s.proctime AS cat
    ON s.category3_id = cat.id;
