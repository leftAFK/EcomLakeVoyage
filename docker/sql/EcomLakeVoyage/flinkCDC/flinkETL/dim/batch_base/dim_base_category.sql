-- ============================================================
-- DIM 离线同步 - base_category（分类维度表，展平层级）
-- 批模式，INSERT OVERWRITE 全量覆盖
-- 自连接 3 次展平分类层级：3级 → 2级 → 1级
-- changelog-producer=none，下游 lookup join
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dim/batch_base/dim_base_category.sql
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
SET 'execution.runtime-mode' = 'batch';

-- ========== Paimon Catalog ==========
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon_warehouse'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS dim;

-- ========== Paimon DIM Target（展平层级宽表） ==========
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
-- ========== 同步作业（自连接展平分类层级） ==========
-- c3 = 3级分类（叶子），c2 = 2级分类（c3的父），c1 = 1级分类（c2的父）
-- 对于 level=3 的行：category3/2/1 全部填充
-- 对于 level=2 的行：category2/1 填充，category3 为 NULL
-- 对于 level=1 的行：仅 category1 填充，其余为 NULL
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
