-- ============================================================
-- 第三部分：5 大分析模型
-- ============================================================

-- ============ 模型 1：用户留存分析 ============

-- 1.1 DWS MV: 每日活跃用户集（浏览）
CREATE MATERIALIZED VIEW IF NOT EXISTS dws.daily_active_users_mv
BUILD IMMEDIATE
REFRESH COMPLETE ON SCHEDULE EVERY 1 DAY
DISTRIBUTED BY HASH(dt) BUCKETS 4
PROPERTIES ("replication_allocation" = "tag.location.default: 1")
AS
SELECT
    CAST(dt AS DATE)                           AS dt,
    user_id,
    MIN(create_time)                           AS first_action_time,
    COUNT(*)                                    AS page_view_count
FROM paimon.dwd.log_page_view
WHERE user_id IS NOT NULL
GROUP BY CAST(dt AS DATE), user_id;

-- SELECT * FROM dws.daily_active_users_mv;

-- 1.2 DWS MV: 每日下单用户集
CREATE MATERIALIZED VIEW IF NOT EXISTS dws.daily_order_users_mv
BUILD IMMEDIATE
REFRESH COMPLETE ON SCHEDULE EVERY 1 DAY
DISTRIBUTED BY HASH(dt) BUCKETS 4
PROPERTIES ("replication_allocation" = "tag.location.default: 1")
AS
SELECT
    CAST(dt AS DATE)                           AS dt,
    user_id,
    MIN(create_time)                           AS first_order_time,
    COUNT(*)                                    AS order_count,
    CAST(SUM(total_amount) AS DECIMAL(18,2))    AS total_amount
FROM paimon.dwd.order_info
WHERE user_id IS NOT NULL
GROUP BY CAST(dt AS DATE), user_id;

-- SELECT * FROM dws.daily_order_users_mv;

-- 2.1 DWS MV: 页面跳转统计
CREATE MATERIALIZED VIEW IF NOT EXISTS dws.page_transition_stats_mv
BUILD IMMEDIATE
REFRESH COMPLETE ON SCHEDULE EVERY 5 MINUTE
DISTRIBUTED BY HASH(dt) BUCKETS 4
PROPERTIES ("replication_allocation" = "tag.location.default: 1")
AS
SELECT
    CAST(dt AS DATE)        AS dt,
    last_page_id,
    page_id,
    COUNT(*)                 AS transition_count,
    COUNT(DISTINCT user_id)  AS transition_users,
    COUNT(DISTINCT mid)      AS transition_sessions
FROM paimon.dwd.log_page_view
WHERE last_page_id IS NOT NULL AND last_page_id != ''
GROUP BY CAST(dt AS DATE), last_page_id, page_id;

-- SELECT * FROM dws.page_transition_stats_mv LIMIT 40;


-- 4.2 DWS MV: GMV 小时级趋势
CREATE MATERIALIZED VIEW IF NOT EXISTS dws.gmv_hourly_trend_mv
BUILD IMMEDIATE
REFRESH COMPLETE ON SCHEDULE EVERY 5 MINUTE
DISTRIBUTED BY HASH(dt) BUCKETS 4
PROPERTIES ("replication_allocation" = "tag.location.default: 1")
AS
SELECT
    CAST(dt AS DATE)                              AS dt,
    HOUR(create_time)                             AS hour,
    COUNT(*)                                       AS order_count,
    COUNT(DISTINCT user_id)                        AS order_users,
    CAST(SUM(total_amount) AS DECIMAL(18,2))       AS gmv,
    CAST(SUM(original_total_amount) AS DECIMAL(18,2)) AS original_amount,
    CAST(SUM(coupon_reduce_amount) AS DECIMAL(18,2))  AS coupon_reduce
FROM paimon.dwd.order_info
WHERE user_id IS NOT NULL
GROUP BY CAST(dt AS DATE), HOUR(create_time);

SELECT * FROM dws.gmv_hourly_trend_mv LIMIT 40;