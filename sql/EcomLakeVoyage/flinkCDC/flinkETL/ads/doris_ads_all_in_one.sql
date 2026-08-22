-- ============================================================
-- Doris ADS 层一键初始化脚本（合并版）
-- 包含：基础 ADS 视图 + 日志 ADS 视图 + 5 大分析模型
--
-- 执行方式：
--   docker cp doris_ads_all_in_one.sql mysql:/tmp/
--   docker exec -i mysql sh -c 'mysql -h doris-fe -P 9030 -u root < /tmp/doris_ads_all_in_one.sql'
-- ============================================================

CREATE DATABASE IF NOT EXISTS ads;
USE ads;

-- ============================================================
-- 第一部分：基础 ADS 视图（7 个）
-- ============================================================

-- 1. 每日交易总览（实时大屏）
CREATE VIEW IF NOT EXISTS ads.daily_trade_overview AS
SELECT
    dt,
    SUM(order_count)            AS total_order_count,
    SUM(order_total_amount)     AS total_order_amount,
    SUM(order_original_amount)  AS total_original_amount,
    SUM(order_coupon_reduce)    AS total_coupon_reduce,
    SUM(payment_count)          AS total_payment_count,
    SUM(payment_total_amount)   AS total_payment_amount,
    SUM(refund_count)           AS total_refund_count,
    SUM(refund_total_amount)    AS total_refund_amount,
    SUM(coupon_count)           AS total_coupon_count,
    SUM(coupon_reduce_amount)   AS total_coupon_reduce_amount
FROM dws.trade_user_stats
GROUP BY dt;

-- SELECT * FROM ads.daily_trade_overview LIMIT 100;
-- 2. 用户交易排行 TOP N
CREATE VIEW IF NOT EXISTS ads.user_trade_ranking AS
SELECT
    dt,
    user_id,
    user_nick_name,
    user_level,
    order_count,
    order_total_amount,
    payment_total_amount,
    refund_total_amount,
    ROW_NUMBER() OVER (PARTITION BY dt ORDER BY order_total_amount DESC) AS rn_order,
    ROW_NUMBER() OVER (PARTITION BY dt ORDER BY payment_total_amount DESC) AS rn_payment
FROM dws.trade_user_stats;

-- 3. 商品销售排行 TOP N
CREATE VIEW IF NOT EXISTS ads.sku_sales_ranking AS
SELECT
    dt,
    sku_id,
    sku_name,
    spu_name,
    category3_name,
    category2_name,
    category1_name,
    brand_name,
    order_count,
    order_sku_num,
    order_total_amount,
    cart_count,
    ROW_NUMBER() OVER (PARTITION BY dt ORDER BY order_total_amount DESC) AS rn_amount,
    ROW_NUMBER() OVER (PARTITION BY dt ORDER BY order_sku_num DESC)    AS rn_qty
FROM dws.trade_sku_stats;

-- 4. 地区交易汇总
CREATE VIEW IF NOT EXISTS ads.region_trade_overview AS
SELECT
    dt,
    province_id,
    region_name,
    big_region,
    order_count,
    order_total_amount,
    order_original_amount,
    order_coupon_reduce,
    ROW_NUMBER() OVER (PARTITION BY dt ORDER BY order_total_amount DESC) AS rn
FROM dws.trade_region_stats_mv;

-- 5. 优惠券核销分析
CREATE VIEW IF NOT EXISTS ads.coupon_analysis AS
SELECT
    dt,
    coupon_id,
    coupon_name,
    coupon_type,
    full_amount,
    reduce_amount,
    use_condition,
    coupon_count,
    coupon_reduce_amount
FROM dws.trade_coupon_stats_mv;

-- 6. 一级品类销售汇总
CREATE VIEW IF NOT EXISTS ads.category1_sales_summary AS
SELECT
    dt,
    category1_id,
    category1_name,
    COUNT(DISTINCT sku_id)     AS sku_count,
    SUM(order_count)           AS order_count,
    SUM(order_sku_num)         AS order_sku_num,
    SUM(order_total_amount)    AS order_total_amount,
    SUM(order_coupon_reduce)   AS order_coupon_reduce,
    SUM(order_activity_reduce) AS order_activity_reduce,
    SUM(cart_count)            AS cart_count,
    SUM(cart_sku_num)          AS cart_sku_num
FROM dws.trade_sku_stats
GROUP BY dt, category1_id, category1_name;

-- 7. 品牌销售汇总
CREATE VIEW IF NOT EXISTS ads.brand_sales_summary AS
SELECT
    dt,
    brand_id,
    brand_name,
    COUNT(DISTINCT sku_id)     AS sku_count,
    SUM(order_count)           AS order_count,
    SUM(order_sku_num)         AS order_sku_num,
    SUM(order_total_amount)    AS order_total_amount,
    SUM(cart_count)            AS cart_count
FROM dws.trade_sku_stats
GROUP BY dt, brand_id, brand_name;

-- ============================================================
-- 第二部分：日志 ADS 视图（6 个）
-- ============================================================

-- 8. 每日流量总览
CREATE VIEW IF NOT EXISTS ads.log_traffic_overview AS
SELECT
    dt,
    SUM(pv)                AS total_pv,
    SUM(uv)                AS total_uv,
    SUM(total_during_time) AS total_during_time,
    CASE WHEN SUM(pv) > 0
         THEN SUM(total_during_time) / SUM(pv)
         ELSE 0 END        AS avg_during_time,
    SUM(total_jump_count)  AS total_jump_count
FROM dws.log_page_stats
GROUP BY dt;

-- 9. 页面流量排行
CREATE VIEW IF NOT EXISTS ads.log_page_ranking AS
SELECT
    dt,
    page_id,
    page_name,
    pv,
    uv,
    total_during_time,
    CASE WHEN pv > 0 THEN total_during_time / pv ELSE 0 END AS avg_during_time,
    ROW_NUMBER() OVER (PARTITION BY dt ORDER BY pv DESC) AS rn_pv,
    ROW_NUMBER() OVER (PARTITION BY dt ORDER BY uv DESC) AS rn_uv
FROM dws.log_page_stats;

-- 10. SKU 曝光排行
CREATE VIEW IF NOT EXISTS ads.log_sku_exposure_ranking AS
SELECT
    dt,
    sku_id,
    sku_name,
    spu_name,
    category3_name,
    category2_name,
    category1_name,
    brand_name,
    exposure_count,
    exposure_user_count,
    ROW_NUMBER() OVER (PARTITION BY dt ORDER BY exposure_count DESC)      AS rn_exposure,
    ROW_NUMBER() OVER (PARTITION BY dt ORDER BY exposure_user_count DESC) AS rn_exposure_user
FROM dws.log_sku_exposure_stats;

-- 11. 用户活跃度排行
CREATE VIEW IF NOT EXISTS ads.log_user_activity_ranking AS
SELECT
    dt,
    user_id,
    user_nick_name,
    user_level,
    startup_count,
    page_view_count,
    action_count,
    exposure_count,
    error_count,
    (startup_count + page_view_count + action_count + exposure_count) AS total_action_count,
    ROW_NUMBER() OVER (PARTITION BY dt ORDER BY 
        (startup_count + page_view_count + action_count + exposure_count) DESC) AS rn
FROM dws.log_user_action_stats;

-- 12. 错误日志分析（物化视图）
CREATE MATERIALIZED VIEW IF NOT EXISTS dws.log_error_stats_mv
BUILD IMMEDIATE
REFRESH COMPLETE ON SCHEDULE EVERY 1 MINUTE
DISTRIBUTED BY HASH(dt) BUCKETS 4
PROPERTIES ("replication_allocation" = "tag.location.default: 1")
AS
SELECT
    dt,
    err_code,
    MAX(err_name)           AS err_name,
    COUNT(*)                 AS error_count,
    COUNT(DISTINCT user_id)  AS error_user_count,
    COUNT(DISTINCT mid)      AS error_device_count
FROM paimon.dwd.log_error
GROUP BY dt, err_code;

-- SELECT * FROM dws.log_error_stats_mv LIMIT 40;

CREATE VIEW IF NOT EXISTS ads.log_error_analysis AS
SELECT
    dt,
    err_code,
    err_name,
    error_count,
    error_user_count,
    error_device_count
FROM dws.log_error_stats_mv;

-- 13. 用户行为漏斗分析
CREATE VIEW IF NOT EXISTS ads.log_funnel_analysis AS
SELECT
    dt,
    COUNT(DISTINCT user_id) AS browse_users,
    COUNT(DISTINCT CASE WHEN did_action   = 1 THEN user_id END) AS action_users,
    COUNT(DISTINCT CASE WHEN did_cart     = 1 THEN user_id END) AS cart_users,
    COUNT(DISTINCT CASE WHEN did_order    = 1 THEN user_id END) AS order_users
FROM (
    SELECT
        user_id,
        dt,
        MAX(browse)  AS did_browse,
        MAX(action)  AS did_action,
        MAX(cart)    AS did_cart,
        MAX(ordered) AS did_order
    FROM (
        SELECT user_id, dt,
               CAST(1 AS INT) AS browse, CAST(0 AS INT) AS action,
               CAST(0 AS INT) AS cart,   CAST(0 AS INT) AS ordered
        FROM paimon.dwd.log_page_view
        WHERE user_id IS NOT NULL AND page_id LIKE 'sku%'
        UNION ALL
        SELECT user_id, dt,
               CAST(0 AS INT) AS browse, CAST(1 AS INT) AS action,
               CAST(0 AS INT) AS cart,   CAST(0 AS INT) AS ordered
        FROM paimon.dwd.log_action
        WHERE user_id IS NOT NULL AND item_type = 'sku'
        UNION ALL
        SELECT user_id, dt,
               CAST(0 AS INT) AS browse, CAST(0 AS INT) AS action,
               CAST(1 AS INT) AS cart,   CAST(0 AS INT) AS ordered
        FROM paimon.dwd.cart_info
        WHERE user_id IS NOT NULL
        UNION ALL
        SELECT user_id, dt,
               CAST(0 AS INT) AS browse, CAST(0 AS INT) AS action,
               CAST(0 AS INT) AS cart,   CAST(1 AS INT) AS ordered
        FROM paimon.dwd.order_info
        WHERE user_id IS NOT NULL
    ) raw_funnel
    GROUP BY user_id, dt
) funnel
GROUP BY dt;



-- 1.3 ADS 视图: 用户首次活跃日
CREATE VIEW IF NOT EXISTS ads.user_first_active AS
SELECT
    user_id,
    MIN(dt) AS first_active_dt
FROM dws.daily_active_users_mv
GROUP BY user_id;

-- 1.4 ADS 视图: 用户首次下单日
CREATE VIEW IF NOT EXISTS ads.user_first_order AS
SELECT
    user_id,
    MIN(dt) AS first_order_dt
FROM dws.daily_order_users_mv
GROUP BY user_id;

-- 1.5 ADS 视图: 浏览留存分析
CREATE VIEW IF NOT EXISTS ads.user_browse_retention AS
WITH base_count AS (
    SELECT first_active_dt, COUNT(*) AS new_users
    FROM ads.user_first_active
    GROUP BY first_active_dt
),
retention AS (
    SELECT
        f.first_active_dt,
        DATEDIFF(d.dt, f.first_active_dt) AS day_diff,
        COUNT(DISTINCT d.user_id)         AS retained_users
    FROM ads.user_first_active f
    JOIN dws.daily_active_users_mv d
      ON f.user_id = d.user_id
    WHERE DATEDIFF(d.dt, f.first_active_dt) IN (0, 1, 7, 30)
    GROUP BY f.first_active_dt, DATEDIFF(d.dt, f.first_active_dt)
)
SELECT
    r.first_active_dt                           AS dt,
    b.new_users                                 AS new_users,
    MAX(CASE WHEN r.day_diff = 1  THEN r.retained_users ELSE 0 END) AS day1_retained,
    MAX(CASE WHEN r.day_diff = 7  THEN r.retained_users ELSE 0 END) AS day7_retained,
    MAX(CASE WHEN r.day_diff = 30 THEN r.retained_users ELSE 0 END) AS day30_retained,
    ROUND(MAX(CASE WHEN r.day_diff = 1  THEN r.retained_users ELSE 0 END) * 100.0 / b.new_users, 2) AS day1_retention_rate,
    ROUND(MAX(CASE WHEN r.day_diff = 7  THEN r.retained_users ELSE 0 END) * 100.0 / b.new_users, 2) AS day7_retention_rate,
    ROUND(MAX(CASE WHEN r.day_diff = 30 THEN r.retained_users ELSE 0 END) * 100.0 / b.new_users, 2) AS day30_retention_rate
FROM retention r
JOIN base_count b ON r.first_active_dt = b.first_active_dt
GROUP BY r.first_active_dt, b.new_users
ORDER BY r.first_active_dt DESC;

-- 1.6 ADS 视图: 购买留存分析
CREATE VIEW IF NOT EXISTS ads.user_purchase_retention AS
WITH base_count AS (
    SELECT first_order_dt, COUNT(*) AS new_buyers
    FROM ads.user_first_order
    GROUP BY first_order_dt
),
retention AS (
    SELECT
        f.first_order_dt,
        DATEDIFF(d.dt, f.first_order_dt) AS day_diff,
        COUNT(DISTINCT d.user_id)         AS retained_users
    FROM ads.user_first_order f
    JOIN dws.daily_order_users_mv d
      ON f.user_id = d.user_id
    WHERE DATEDIFF(d.dt, f.first_order_dt) IN (0, 1, 7, 30)
    GROUP BY f.first_order_dt, DATEDIFF(d.dt, f.first_order_dt)
)
SELECT
    r.first_order_dt                            AS dt,
    b.new_buyers                                AS new_buyers,
    MAX(CASE WHEN r.day_diff = 1  THEN r.retained_users ELSE 0 END) AS day1_repurchase,
    MAX(CASE WHEN r.day_diff = 7  THEN r.retained_users ELSE 0 END) AS day7_repurchase,
    MAX(CASE WHEN r.day_diff = 30 THEN r.retained_users ELSE 0 END) AS day30_repurchase,
    ROUND(MAX(CASE WHEN r.day_diff = 1  THEN r.retained_users ELSE 0 END) * 100.0 / b.new_buyers, 2) AS day1_repurchase_rate,
    ROUND(MAX(CASE WHEN r.day_diff = 7  THEN r.retained_users ELSE 0 END) * 100.0 / b.new_buyers, 2) AS day7_repurchase_rate,
    ROUND(MAX(CASE WHEN r.day_diff = 30 THEN r.retained_users ELSE 0 END) * 100.0 / b.new_buyers, 2) AS day30_repurchase_rate
FROM retention r
JOIN base_count b ON r.first_order_dt = b.first_order_dt
GROUP BY r.first_order_dt, b.new_buyers
ORDER BY r.first_order_dt DESC;

-- ============ 模型 2：用户路径分析 ============



-- 2.2 ADS 视图: 页面跳转矩阵
CREATE VIEW IF NOT EXISTS ads.page_transition_matrix AS
SELECT
    dt,
    last_page_id             AS from_page,
    page_id                  AS to_page,
    transition_count,
    transition_users,
    transition_sessions,
    CASE WHEN page_id = '' OR page_id IS NULL THEN 1 ELSE 0 END AS is_bounce,
    ROW_NUMBER() OVER (PARTITION BY dt ORDER BY transition_count DESC) AS rn
FROM dws.page_transition_stats_mv;

-- 2.3 ADS 视图: TOP 10 热门路径
CREATE VIEW IF NOT EXISTS ads.user_path_top10 AS
SELECT
    dt,
    CONCAT(from_page, ' -> ', to_page) AS path,
    transition_count,
    transition_users,
    rn
FROM ads.page_transition_matrix
WHERE rn <= 10
ORDER BY dt DESC, rn;

-- 2.4 ADS 视图: 页面流出分析
CREATE VIEW IF NOT EXISTS ads.page_outflow_top3 AS
SELECT *
FROM (
    SELECT
        dt,
        last_page_id             AS from_page,
        page_id                  AS to_page,
        transition_count,
        transition_users,
        ROW_NUMBER() OVER (
            PARTITION BY dt, last_page_id 
            ORDER BY transition_count DESC
        ) AS rn
    FROM dws.page_transition_stats_mv
) t
WHERE rn <= 3
ORDER BY dt DESC, from_page, rn;

-- ============ 模型 3：RFM 用户分层 ============

-- 3.1 ADS 视图: RFM 用户分层
CREATE VIEW IF NOT EXISTS ads.rfm_user_segmentation AS
WITH rfm_raw AS (
    SELECT
        user_id,
        MAX(user_nick_name)                          AS user_nick_name,
        MAX(user_level)                              AS user_level,
        DATEDIFF(CURDATE(), MAX(dt))                 AS recency,
        CAST(SUM(order_count) AS BIGINT)             AS frequency,
        CAST(SUM(order_total_amount) AS DECIMAL(18,2)) AS monetary
    FROM dws.trade_user_stats
    WHERE order_count > 0
    GROUP BY user_id
),
avg_metrics AS (
    SELECT
        AVG(recency)   AS avg_r,
        AVG(frequency) AS avg_f,
        AVG(monetary)  AS avg_m
    FROM rfm_raw
)
SELECT
    r.user_id,
    r.user_nick_name,
    r.user_level,
    r.recency,
    r.frequency,
    r.monetary,
    CASE WHEN r.recency <= a.avg_r THEN 1 ELSE 0 END  AS r_score,
    CASE WHEN r.frequency >= a.avg_f THEN 1 ELSE 0 END AS f_score,
    CASE WHEN r.monetary >= a.avg_m THEN 1 ELSE 0 END AS m_score,
    CASE
        WHEN r.recency <= a.avg_r AND r.frequency >= a.avg_f AND r.monetary >= a.avg_m THEN '高价值用户'
        WHEN r.recency >  a.avg_r AND r.frequency >= a.avg_f AND r.monetary >= a.avg_m THEN '重点保持用户'
        WHEN r.recency <= a.avg_r AND r.frequency <  a.avg_f AND r.monetary >= a.avg_m THEN '重点发展用户'
        WHEN r.recency >  a.avg_r AND r.frequency <  a.avg_f AND r.monetary >= a.avg_m THEN '重点挽留用户'
        WHEN r.recency <= a.avg_r AND r.frequency >= a.avg_f AND r.monetary <  a.avg_m THEN '一般价值用户'
        WHEN r.recency >  a.avg_r AND r.frequency >= a.avg_f AND r.monetary <  a.avg_m THEN '一般保持用户'
        WHEN r.recency <= a.avg_r AND r.frequency <  a.avg_f AND r.monetary <  a.avg_m THEN '一般发展用户'
        ELSE '流失预警用户'
    END AS user_segment
FROM rfm_raw r
CROSS JOIN avg_metrics a;

-- 3.2 ADS 视图: RFM 分层汇总
CREATE VIEW IF NOT EXISTS ads.rfm_segment_summary AS
SELECT
    user_segment,
    COUNT(*)                                      AS user_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS user_pct,
    SUM(frequency)                                 AS total_orders,
    SUM(monetary)                                  AS total_amount,
    ROUND(SUM(monetary) * 100.0 / SUM(SUM(monetary)) OVER(), 2) AS amount_pct,
    ROUND(AVG(recency), 1)                         AS avg_recency,
    ROUND(AVG(frequency), 1)                      AS avg_frequency,
    ROUND(AVG(monetary), 2)                        AS avg_monetary
FROM ads.rfm_user_segmentation
GROUP BY user_segment
ORDER BY total_amount DESC;

-- ============ 模型 4：GMV 实时大屏 ============

-- 4.1 ADS 视图: GMV 实时大屏 - 日级总览
CREATE VIEW IF NOT EXISTS ads.gmv_realtime_dashboard AS
SELECT
    dt,
    SUM(order_count)            AS total_order_count,
    SUM(order_total_amount)     AS total_gmv,
    SUM(order_original_amount)  AS total_original_amount,
    SUM(order_coupon_reduce)    AS total_coupon_reduce,
    SUM(payment_count)          AS total_payment_count,
    SUM(payment_total_amount)   AS total_payment_amount,
    SUM(refund_count)           AS total_refund_count,
    SUM(refund_total_amount)    AS total_refund_amount,
    SUM(coupon_count)           AS total_coupon_count,
    SUM(coupon_reduce_amount)   AS total_coupon_reduce_amount,
    CASE WHEN SUM(order_count) > 0
         THEN ROUND(SUM(payment_count) * 100.0 / SUM(order_count), 2)
         ELSE 0 END AS payment_conversion_rate,
    CASE WHEN SUM(order_count) > 0
         THEN ROUND(SUM(refund_count) * 100.0 / SUM(order_count), 2)
         ELSE 0 END AS refund_rate,
    CASE WHEN SUM(order_count) > 0
         THEN ROUND(SUM(order_total_amount) / SUM(order_count), 2)
         ELSE 0 END AS avg_order_amount,
    CASE WHEN SUM(order_original_amount) > 0
         THEN ROUND(SUM(order_coupon_reduce) * 100.0 / SUM(order_original_amount), 2)
         ELSE 0 END AS coupon_usage_rate
FROM dws.trade_user_stats
GROUP BY dt;



-- 4.3 ADS 视图: GMV 小时级趋势
CREATE VIEW IF NOT EXISTS ads.gmv_hourly_trend AS
SELECT
    dt,
    hour,
    order_count,
    order_users,
    gmv,
    original_amount,
    coupon_reduce,
    CASE WHEN LAG(gmv, 1, 0) OVER (PARTITION BY dt ORDER BY hour) > 0
         THEN ROUND(
             (gmv - LAG(gmv, 1, 0) OVER (PARTITION BY dt ORDER BY hour)) * 100.0
             / LAG(gmv, 1, 0) OVER (PARTITION BY dt ORDER BY hour), 2)
         ELSE 0 END AS gmv_hourly_growth_rate,
    SUM(gmv) OVER (PARTITION BY dt ORDER BY hour ROWS UNBOUNDED PRECEDING) AS cumulative_gmv
FROM dws.gmv_hourly_trend_mv
ORDER BY dt DESC, hour;

-- 4.4 ADS 视图: GMV 今日概要
CREATE VIEW IF NOT EXISTS ads.gmv_today_summary AS
SELECT
    dt,
    total_order_count,
    total_gmv,
    total_payment_amount,
    total_refund_amount,
    payment_conversion_rate,
    refund_rate,
    avg_order_amount,
    coupon_usage_rate
FROM ads.gmv_realtime_dashboard
WHERE dt = CURDATE();

-- ============ 模型 5：商品销售趋势 ============

-- 5.1 ADS 视图: 商品销售趋势（含 7 日均线）
CREATE VIEW IF NOT EXISTS ads.sku_sales_trend AS
SELECT
    dt,
    sku_id,
    sku_name,
    spu_name,
    category1_name,
    brand_name,
    order_count,
    order_sku_num,
    order_total_amount,
    cart_count,
    ROUND(
        AVG(order_count) OVER (
            PARTITION BY sku_id
            ORDER BY dt
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 1
    ) AS avg_7d_order_count,
    ROUND(
        AVG(order_total_amount) OVER (
            PARTITION BY sku_id
            ORDER BY dt
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2
    ) AS avg_7d_amount,
    ROUND(
        AVG(cart_count) OVER (
            PARTITION BY sku_id
            ORDER BY dt
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 1
    ) AS avg_7d_cart_count,
    CASE WHEN LAG(order_count, 1, 0) OVER (PARTITION BY sku_id ORDER BY dt) > 0
         THEN ROUND(
             (order_count - LAG(order_count, 1, 0) OVER (PARTITION BY sku_id ORDER BY dt)) * 100.0
             / LAG(order_count, 1, 0) OVER (PARTITION BY sku_id ORDER BY dt), 2)
         ELSE 0 END AS daily_growth_rate
FROM dws.trade_sku_stats;

-- 5.2 ADS 视图: 商品销量异常预警
CREATE VIEW IF NOT EXISTS ads.sku_sales_anomaly AS
SELECT
    dt,
    sku_id,
    sku_name,
    spu_name,
    category1_name,
    brand_name,
    order_count,
    avg_7d_order_count,
    order_total_amount,
    avg_7d_amount,
    CASE WHEN avg_7d_order_count > 0
         THEN ROUND((order_count - avg_7d_order_count) * 100.0 / avg_7d_order_count, 2)
         ELSE 0 END AS deviation_rate,
    CASE
        WHEN avg_7d_order_count > 0 AND order_count > avg_7d_order_count * 1.3 THEN '销量激增'
        WHEN avg_7d_order_count > 0 AND order_count < avg_7d_order_count * 0.7 THEN '销量下滑'
        WHEN avg_7d_order_count = 0 AND order_count > 0 THEN '新品爆发'
        ELSE '正常'
    END AS anomaly_type
FROM ads.sku_sales_trend
WHERE avg_7d_order_count > 0
  AND (
      order_count > avg_7d_order_count * 1.3
      OR order_count < avg_7d_order_count * 0.7
  )
ORDER BY dt DESC, deviation_rate DESC;
-- 5.3 ADS 视图: 品类销售趋势汇总
CREATE VIEW IF NOT EXISTS ads.category_sales_trend AS
SELECT
    dt,
    category1_name,
    SUM(order_count)                              AS total_order_count,
    SUM(order_sku_num)                            AS total_sku_num,
    SUM(order_total_amount)                       AS total_amount,
    SUM(cart_count)                               AS total_cart_count,
    ROUND(
        AVG(SUM(order_count)) OVER (
            PARTITION BY category1_name
            ORDER BY dt
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 1
    ) AS avg_7d_order_count,
    ROUND(
        AVG(SUM(order_total_amount)) OVER (
            PARTITION BY category1_name
            ORDER BY dt
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2
    ) AS avg_7d_amount,
    ROUND(
        SUM(order_total_amount) * 100.0
        / SUM(SUM(order_total_amount)) OVER (PARTITION BY dt), 2
    ) AS amount_pct
FROM dws.trade_sku_stats
GROUP BY dt, category1_name
ORDER BY dt DESC, total_amount DESC;



