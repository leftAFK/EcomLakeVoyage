-- ============================================================
-- DWD 实时同步 - order_detail（订单明细宽表）
-- 流模式，INSERT INTO 持续同步
-- changelog-producer=input，dt 分区
--
-- Join 策略：
--   dim.sku_info     → Lookup Join (处理时间，维度变更频率低)
--   dim.coupon_info  → Lookup Join (处理时间，changelog-producer=none)
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dwd/business/dwd_order_detail.sql
-- ============================================================
-- 前置条件：dim.sku_info + dim.coupon_info 必须先执行
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

-- ========== Paimon DWD Target（订单明细宽表） ==========
CREATE TABLE IF NOT EXISTS dwd.order_detail (
    id                    BIGINT,
    order_id              BIGINT,
    order_line_no         INT,
    sku_id                BIGINT,
    sku_name              STRING,
    spu_id                BIGINT,
    spu_name              STRING,
    category3_id          BIGINT,
    category3_name        STRING,
    category2_id          BIGINT,
    category2_name        STRING,
    category1_id          BIGINT,
    category1_name        STRING,
    brand_id              BIGINT,
    brand_name            STRING,
    img_url               STRING,
    order_price           DECIMAL(18,2),
    sku_num               INT,
    create_time           TIMESTAMP(3),
    source_type           SMALLINT,
    source_id             BIGINT,
    split_activity_amount DECIMAL(18,4),
    coupon_id             BIGINT,
    coupon_name           STRING,
    coupon_type           TINYINT,
    split_coupon_amount   DECIMAL(18,4),
    split_total_amount    DECIMAL(18,4),
    update_time           TIMESTAMP(3),
    dt                    STRING,
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
-- ========== 同步作业（Lookup Join 拼宽表） ==========
-- dim.sku_info: Lookup Join（处理时间，维度变更频率低）
-- dim.coupon_info: Lookup Join（处理时间，changelog-producer=none）
INSERT INTO dwd.order_detail
SELECT
    od.id,
    od.order_id,
    od.order_line_no,
    od.sku_id,
    od.sku_name,
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
    od.img_url,
    od.order_price,
    od.sku_num,
    od.create_time,
    od.source_type,
    od.source_id,
    od.split_activity_amount,
    od.coupon_id,
    cou.coupon_name,
    cou.coupon_type,
    od.split_coupon_amount,
    od.split_total_amount,
    od.update_time,
    od.dt
FROM (
    SELECT *, PROCTIME() AS proctime
    FROM ods.order_detail
) od
-- Lookup Join: 处理时间关联（sku_info 维度变更频率低）
LEFT JOIN dim.sku_info FOR SYSTEM_TIME AS OF od.proctime AS sku
    ON od.sku_id = sku.id
-- Lookup Join: 处理时间关联（coupon_info 无 changelog）
LEFT JOIN dim.coupon_info FOR SYSTEM_TIME AS OF od.proctime AS cou
    ON od.coupon_id = cou.id;
