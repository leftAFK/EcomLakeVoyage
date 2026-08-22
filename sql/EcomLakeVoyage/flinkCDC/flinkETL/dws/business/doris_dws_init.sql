-- ============================================================
-- Doris DWS 层初始化脚本
-- 在 Doris MySQL Client 中执行（不是 Flink SQL Client）
--
-- 包含：
--   1. DWS 数据库 + Unique Key 表（Flink 实时写入）
--   2. Paimon Catalog（Doris 读 Paimon DWD）
--   3. DWS 物化视图（地区/优惠券，定时刷新读 Paimon）
--
-- 执行方式：
--   mysql -h <doris-fe-host> -P 9030 -u root < doris_dws_init.sql
-- ============================================================

-- ============================================================
-- 第一部分：DWS 数据库 + Unique Key 表（Flink 实时写入）
-- ============================================================

CREATE DATABASE IF NOT EXISTS dws;

-- ========== DWS: 用户交易汇总表（Flink 写入） ==========
-- Unique Key 模型：支持 Flink changelog 的 upsert/delete
-- Key 列必须在最前面：user_id, dt
CREATE TABLE IF NOT EXISTS dws.trade_user_stats (
    user_id                  BIGINT,
    dt                       DATE,
    user_nick_name           VARCHAR(100),
    user_level               TINYINT,
    age_range                VARCHAR(20),
    gender                   TINYINT,
    order_count              BIGINT,
    order_total_amount       DECIMAL(18,4),
    order_original_amount    DECIMAL(18,4),
    order_coupon_reduce      DECIMAL(18,4),
    payment_count            BIGINT,
    payment_total_amount     DECIMAL(18,4),
    refund_count             BIGINT,
    refund_total_amount      DECIMAL(18,4),
    refund_num               INT,
    coupon_count             BIGINT,
    coupon_reduce_amount     DECIMAL(18,4)
)
UNIQUE KEY(user_id, dt)
PARTITION BY RANGE (dt) ()
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);

-- ========== DWS: 商品交易汇总表（Flink 写入） ==========
CREATE TABLE IF NOT EXISTS dws.trade_sku_stats (
    sku_id                   BIGINT,
    dt                       DATE,
    sku_name                 VARCHAR(200),
    spu_id                   BIGINT,
    spu_name                 VARCHAR(200),
    category3_id             BIGINT,
    category3_name           VARCHAR(100),
    category2_id             BIGINT,
    category2_name           VARCHAR(100),
    category1_id             BIGINT,
    category1_name           VARCHAR(100),
    brand_id                 BIGINT,
    brand_name               VARCHAR(100),
    order_count              BIGINT,
    order_sku_num            INT,
    order_total_amount       DECIMAL(18,4),
    order_coupon_reduce      DECIMAL(18,4),
    order_activity_reduce    DECIMAL(18,4),
    cart_count               BIGINT,
    cart_sku_num             INT
)
UNIQUE KEY(sku_id, dt)
PARTITION BY RANGE (dt) ()
DISTRIBUTED BY HASH(sku_id) BUCKETS 8
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);


-- ============================================================
-- 第二部分：Paimon Catalog（Doris 读 Paimon DWD）
-- ============================================================

-- 创建 Paimon Catalog
-- warehouse 路径需与 Flink 容器中 Paimon 仓库路径一致
-- 注意：Doris BE 节点必须能访问该路径（Docker 挂载卷需共享）
CREATE CATALOG IF NOT EXISTS paimon properties (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon_warehouse',
    "paimon.catalog.type" = "filesystem"
);

-- ============================================================
-- 第三部分：DWS 物化视图（定时刷新读 Paimon DWD）
-- 地区和优惠券汇总：单源聚合，适合 Doris 物化视图
-- ============================================================

-- ========== DWS MV: 地区交易汇总 ==========
-- 每分钟刷新，从 Paimon DWD order_info 聚合
-- 注意：Paimon 是外部表，Doris 无法感知分区变更，用 COMPLETE 全量刷新
USE dws;
CREATE MATERIALIZED VIEW trade_region_stats_mv
BUILD IMMEDIATE
REFRESH COMPLETE ON SCHEDULE EVERY 1 MINUTE
DISTRIBUTED BY HASH(province_id) BUCKETS 4
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
)
AS
SELECT
    province_id,
    dt,
    MAX(region_name)           AS region_name,
    MAX(big_region)            AS big_region,
    COUNT(*)                   AS order_count,
    CAST(SUM(total_amount) AS DECIMAL(18,4))          AS order_total_amount,
    CAST(SUM(original_total_amount) AS DECIMAL(18,4)) AS order_original_amount,
    CAST(SUM(coupon_reduce_amount) AS DECIMAL(18,4))  AS order_coupon_reduce
FROM paimon.dwd.order_info
GROUP BY province_id, dt;


-- ========== DWS MV: 优惠券汇总 ==========
-- 每分钟刷新，从 Paimon DWD coupon_use 聚合
CREATE MATERIALIZED VIEW trade_coupon_stats_mv
BUILD IMMEDIATE
REFRESH COMPLETE ON SCHEDULE EVERY 1 MINUTE
DISTRIBUTED BY HASH(coupon_id) BUCKETS 4
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
)
AS
SELECT
    coupon_id,
    dt,
    MAX(coupon_name)     AS coupon_name,
    MAX(coupon_type)     AS coupon_type,
    MAX(full_amount)     AS full_amount,
    MAX(reduce_amount)   AS reduce_amount,
    MAX(use_condition)   AS use_condition,
    COUNT(*)             AS coupon_count,
    CAST(SUM(coupon_reduce_amount) AS DECIMAL(18,4)) AS coupon_reduce_amount
FROM paimon.dwd.coupon_use
GROUP BY coupon_id, dt;







