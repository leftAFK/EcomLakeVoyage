-- ============================================================
-- DIM 实时同步合并作业（4 张流式维度表）
-- 用 Statement Set 合并 4 个 INSERT INTO 为 1 个 Flink 作业
-- 占用 slot: 1（常驻流作业）
-- 数据源: Paimon ods.* (流读 changelog)
-- 目标: Paimon dim.* (sku_info/user_info 用 input changelog)
-- 依赖: 必须先跑完 ods_cdc_all.sql + dim_batch_all.sql
--   - dim.sku_info 依赖 ods.sku_info + ods.spu_info + ods.base_brand + dim.base_category
--   - dim.spu_info 依赖 ods.spu_info + ods.base_brand + dim.base_category
--   - dim.coupon_info 依赖 ods.coupon_info
--   - dim.user_info 依赖 ods.user_info
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/combined/dim_streaming_all.sql
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
SET 'execution.runtime-mode' = 'streaming';

-- ========== Paimon Catalog ==========
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon_warehouse'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS dim;

-- ============================================================
-- 1. dim.coupon_info（优惠券维度表，changelog-producer=none）
-- ============================================================
CREATE TABLE IF NOT EXISTS dim.coupon_info (
    id            BIGINT,
    coupon_type   TINYINT,
    full_amount   DECIMAL(18,2),
    reduce_amount DECIMAL(18,2),
    coupon_name   STRING,
    use_condition STRING,
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
-- 2. dim.spu_info（SPU 维度表，拼 brand_name + 三级分类名）
-- ============================================================
CREATE TABLE IF NOT EXISTS dim.spu_info (
    id               BIGINT,
    spu_name         STRING,
    description      STRING,
    category3_id     BIGINT,
    category3_name   STRING,
    category2_id     BIGINT,
    category2_name   STRING,
    category1_id     BIGINT,
    category1_name   STRING,
    brand_id         BIGINT,
    brand_name       STRING,
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
-- 3. dim.sku_info（SKU 维度表，拼 spu/brand/分类，changelog-producer=input）
-- ============================================================
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
-- ============================================================
-- 4. dim.user_info（用户维度表，changelog-producer=input）
-- ============================================================
CREATE TABLE IF NOT EXISTS dim.user_info (
    id          BIGINT,
    login_name  STRING,
    nick_name   STRING,
    name        STRING,
    phone_num   STRING,
    email       STRING,
    user_level  TINYINT,
    birthday    DATE,
    gender      TINYINT,
    age_range   STRING,
    status      SMALLINT,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
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
-- ============================================================
-- Statement Set：合并 4 个 INSERT INTO 为 1 个作业
-- ============================================================
BEGIN STATEMENT SET;

INSERT INTO dim.coupon_info
SELECT id, coupon_type, full_amount, reduce_amount, coupon_name,
       use_condition, create_time, update_time
FROM ods.coupon_info;

INSERT INTO dim.spu_info
SELECT
    s.id,
    s.spu_name,
    s.description,
    s.category3_id,
    cat.category3_name,
    cat.category2_id,
    cat.category2_name,
    cat.category1_id,
    cat.category1_name,
    s.brand_id,
    brand.brand_name,
    s.create_time,
    s.update_time
FROM (
    SELECT *, PROCTIME() AS proctime
    FROM ods.spu_info
) s
LEFT JOIN ods.base_brand FOR SYSTEM_TIME AS OF s.proctime AS brand
    ON s.brand_id = brand.id
LEFT JOIN dim.base_category FOR SYSTEM_TIME AS OF s.proctime AS cat
    ON s.category3_id = cat.id;

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

INSERT INTO dim.user_info
SELECT id, login_name, nick_name, name, phone_num, email, user_level,
       birthday, gender, age_range, status, create_time, update_time
FROM ods.user_info;

END;
