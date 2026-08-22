-- ============================================================
-- DWS 实时汇总 - 商品交易汇总表（Flink 写 Doris）
-- 流模式，UNION ALL 多源聚合
-- 读 Paimon DWD changelog → Flink GROUP BY 增量聚合 → 写 Doris Unique Key 表
--
-- 数据来源：
--   dwd.order_detail → 下单次数、下单件数、下单金额、优惠券减免、活动减免
--   dwd.cart_info    → 加购次数、加购件数
--
-- 聚合粒度：sku_id + dt（每商品每天一行）
-- Doris 表模型：Unique Key（支持 Flink changelog 的 upsert/delete）
--
-- 前置条件：
--   1. dwd.order_detail + dwd.cart_info 必须先执行
--   2. Doris 端 dws.trade_sku_stats 表必须先建好（见 doris_init.sql）
--   3. doris-flink-connector JAR 必须放入 /opt/flink/lib/
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dws/business/dws_trade_sku_stats.sql
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

-- ========== Paimon Catalog（读 DWD 层） ==========
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon_warehouse'
);
USE CATALOG paimon;

-- ========== Doris Sink（临时表，Flink → Doris Stream Load） ==========
-- 注意：fenodes 需替换为你的 Doris FE 地址和 HTTP 端口（默认 8030）
CREATE TEMPORARY TABLE doris_trade_sku_stats (
    sku_id                   BIGINT,
    dt                       DATE,
    sku_name                 STRING,
    spu_id                   BIGINT,
    spu_name                 STRING,
    category3_id             BIGINT,
    category3_name           STRING,
    category2_id             BIGINT,
    category2_name           STRING,
    category1_id             BIGINT,
    category1_name           STRING,
    brand_id                 BIGINT,
    brand_name               STRING,
    order_count              BIGINT,
    order_sku_num            INT,
    order_total_amount       DECIMAL(18,4),
    order_coupon_reduce      DECIMAL(18,4),
    order_activity_reduce    DECIMAL(18,4),
    cart_count               BIGINT,
    cart_sku_num             INT,
    PRIMARY KEY (sku_id, dt) NOT ENFORCED
) WITH (
    'connector' = 'doris',
    'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'dws.trade_sku_stats',
    'username' = 'root',
    'password' = '',
    'sink.label-prefix' = 'dws_trade_sku_stats',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.properties.max_filter_ratio' = '0.1'
);

-- ========== 汇总作业（UNION ALL 2 源聚合 → Doris） ==========
INSERT INTO doris_trade_sku_stats
SELECT
    sku_id,
    CAST(dt AS DATE)         AS dt,
    MAX(sku_name)           AS sku_name,
    MAX(spu_id)             AS spu_id,
    MAX(spu_name)           AS spu_name,
    MAX(category3_id)       AS category3_id,
    MAX(category3_name)     AS category3_name,
    MAX(category2_id)       AS category2_id,
    MAX(category2_name)     AS category2_name,
    MAX(category1_id)       AS category1_id,
    MAX(category1_name)     AS category1_name,
    MAX(brand_id)           AS brand_id,
    MAX(brand_name)         AS brand_name,
    SUM(order_count)        AS order_count,
    SUM(order_sku_num)      AS order_sku_num,
    SUM(order_total_amount) AS order_total_amount,
    SUM(order_coupon_reduce) AS order_coupon_reduce,
    SUM(order_activity_reduce) AS order_activity_reduce,
    SUM(cart_count)         AS cart_count,
    SUM(cart_sku_num)       AS cart_sku_num
FROM (
    -- ===== 下单明细 =====
    SELECT
        sku_id, dt,
        sku_name, spu_id, spu_name,
        category3_id, category3_name, category2_id, category2_name,
        category1_id, category1_name, brand_id, brand_name,
        CAST(1 AS BIGINT)          AS order_count,
        sku_num                    AS order_sku_num,
        CAST(split_total_amount AS DECIMAL(18,4))   AS order_total_amount,
        CAST(split_coupon_amount AS DECIMAL(18,4))  AS order_coupon_reduce,
        CAST(split_activity_amount AS DECIMAL(18,4)) AS order_activity_reduce,
        CAST(0 AS BIGINT)          AS cart_count,
        CAST(0 AS INT)             AS cart_sku_num
    FROM paimon.dwd.order_detail
    WHERE sku_id IS NOT NULL 
      AND dt IS NOT NULL 
    UNION ALL

    -- ===== 加购 =====
    SELECT
        sku_id, dt,
        sku_name, spu_id, spu_name,
        category3_id, category3_name, category2_id, category2_name,
        category1_id, category1_name, brand_id, brand_name,
        CAST(0 AS BIGINT),
        CAST(0 AS INT),
        CAST(0 AS DECIMAL(18,4)),
        CAST(0 AS DECIMAL(18,4)),
        CAST(0 AS DECIMAL(18,4)),
        CAST(1 AS BIGINT),
        sku_num
    FROM paimon.dwd.cart_info
    WHERE sku_id IS NOT NULL 
      AND dt IS NOT NULL 
) t
GROUP BY sku_id, dt;

SELECT * FROM dws.trade_sku_stats;


