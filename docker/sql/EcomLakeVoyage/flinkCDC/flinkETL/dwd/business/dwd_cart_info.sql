-- ============================================================
-- DWD 实时同步 - cart_info（购物车宽表）
-- 流模式，INSERT INTO 持续同步
-- changelog-producer=input，dt 分区
--
-- Join 策略：
--   dim.sku_info  → Lookup Join (处理时间，维度变更频率低)
--   dim.user_info → Lookup Join (处理时间，维度变更频率低)
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/business/dwd_cart_info.sql
-- ============================================================
-- 前置条件：dim.sku_info + dim.user_info 必须先执行
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
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'execution.runtime-mode' = 'streaming';

-- ========== Paimon Catalog ==========
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon_warehouse'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS dwd;

-- ========== Paimon DWD Target（购物车宽表） ==========
CREATE TABLE IF NOT EXISTS dwd.cart_info (
    id              BIGINT,
    user_id         BIGINT,
    user_nick_name  STRING,
    user_level      TINYINT,
    age_range       STRING,
    sku_id          BIGINT,
    sku_name        STRING,
    spu_id          BIGINT,
    spu_name        STRING,
    category3_id    BIGINT,
    category3_name  STRING,
    category2_id    BIGINT,
    category2_name  STRING,
    category1_id    BIGINT,
    category1_name  STRING,
    brand_id        BIGINT,
    brand_name      STRING,
    category_id     BIGINT,
    cart_price      DECIMAL(18,2),
    sku_num         INT,
    img_url         STRING,
    sku_attr        STRING,
    order_id        BIGINT,
    is_checked      BOOLEAN,
    create_time     TIMESTAMP(3),
    operate_time    TIMESTAMP(3),
    update_time     TIMESTAMP(3),
    dt              STRING,
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
-- ========== 同步作业（双 Lookup Join 拼宽表） ==========
-- dim.sku_info: Lookup Join（处理时间，维度变更频率低）
-- dim.user_info: Lookup Join（处理时间，维度变更频率低）
INSERT INTO dwd.cart_info
SELECT
    c.id,
    c.user_id,
    u.nick_name         AS user_nick_name,
    u.user_level,
    u.age_range,
    c.sku_id,
    c.sku_name,
    sku.spu_id,
    sku.spu_name,
    sku.category3_id,
    sku.category3_name,
    sku.category2_id,
    sku.category2_name,
    sku.category1_id,
    sku.category1_name,
    sku.brand_id,
    sku.brand_name,
    c.category_id,
    c.cart_price,
    c.sku_num,
    c.img_url,
    c.sku_attr,
    c.order_id,
    c.is_checked,
    c.create_time,
    c.operate_time,
    c.update_time,
    c.dt
FROM (
    SELECT *, PROCTIME() AS proctime
    FROM ods.cart_info
) c
-- Lookup Join: 处理时间关联（sku_info 维度变更频率低）
LEFT JOIN dim.sku_info FOR SYSTEM_TIME AS OF c.proctime AS sku
    ON c.sku_id = sku.id
-- Lookup Join: 处理时间关联（user_info 维度变更频率低）
LEFT JOIN dim.user_info FOR SYSTEM_TIME AS OF c.proctime AS u
    ON c.user_id = u.id;
