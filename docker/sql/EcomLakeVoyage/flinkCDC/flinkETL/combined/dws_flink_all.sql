-- ============================================================
-- DWS 聚合作业合并（5 个 Flink DWS 作业）
-- 用 Statement Set 合并 5 个 INSERT INTO 为 1 个 Flink 作业
-- 占用 slot: 1（常驻流作业）
-- 数据源: Paimon dwd.* (业务 4 张 + 日志 4 张)
-- 目标: Doris dws.* (Unique 表 + Aggregate 表)
-- 依赖: 必须先跑完 dwd_business_all.sql + dwd_log_all.sql
--   并在 Doris 中执行 doris_dws_init.sql + doris_dws_log_init.sql 建好目标表
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/combined/dws_flink_all.sql
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
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'execution.runtime-mode' = 'streaming';

-- ========== Paimon Catalog ==========
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon_warehouse'
);

-- ============================================================
-- 1. Doris Sink: dws.trade_user_stats（用户交易汇总）
-- ============================================================
CREATE TEMPORARY TABLE doris_trade_user_stats (
    user_id                  BIGINT,
    dt                       DATE,
    user_nick_name           STRING,
    user_level               TINYINT,
    age_range                STRING,
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
    coupon_reduce_amount     DECIMAL(18,4),
    PRIMARY KEY (user_id, dt) NOT ENFORCED
) WITH (
    'connector' = 'doris',
    'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'dws.trade_user_stats',
    'username' = 'root',
    'password' = '',
    'sink.label-prefix' = 'dws_trade_user_stats',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.properties.max_filter_ratio' = '0.1'
);

-- ============================================================
-- 2. Doris Sink: dws.trade_sku_stats（商品交易汇总）
-- ============================================================
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

-- ============================================================
-- 3. Doris Sink: dws.log_page_stats（页面流量汇总）
-- ============================================================
CREATE TABLE dws_log_page_stats (
    page_id              STRING,
    dt                   DATE,
    page_name            STRING,
    pv                   BIGINT,
    uv                   BIGINT,
    total_during_time    BIGINT,
    total_jump_count     INT
) WITH (
    'connector' = 'doris',
    'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'dws.log_page_stats',
    'username' = 'root',
    'password' = '',
    'sink.label-prefix' = 'dws_log_page_stats',
    'sink.enable-delete' = 'true',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true'
);

-- ============================================================
-- 4. Doris Sink: dws.log_sku_exposure_stats（商品曝光汇总）
-- ============================================================
CREATE TABLE dws_log_sku_exposure_stats (
    sku_id               BIGINT,
    dt                   DATE,
    sku_name             STRING,
    spu_name             STRING,
    category3_name       STRING,
    category2_name       STRING,
    category1_name       STRING,
    brand_name           STRING,
    exposure_count       BIGINT,
    exposure_user_count  BIGINT
) WITH (
    'connector' = 'doris',
    'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'dws.log_sku_exposure_stats',
    'username' = 'root',
    'password' = '',
    'sink.label-prefix' = 'dws_log_sku_exposure_stats',
    'sink.enable-delete' = 'true',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true'
);

-- ============================================================
-- 5. Doris Sink: dws.log_user_action_stats（用户行为汇总）
-- ============================================================
CREATE TABLE dws_log_user_action_stats (
    user_id              BIGINT,
    dt                   DATE,
    user_nick_name       STRING,
    user_level           TINYINT,
    age_range            STRING,
    gender               TINYINT,
    startup_count        BIGINT,
    page_view_count      BIGINT,
    action_count         BIGINT,
    exposure_count       BIGINT,
    error_count          BIGINT
) WITH (
    'connector' = 'doris',
    'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'dws.log_user_action_stats',
    'username' = 'root',
    'password' = '',
    'sink.label-prefix' = 'dws_log_user_action_stats',
    'sink.enable-delete' = 'true',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true'
);

-- ============================================================
-- Statement Set：合并 5 个 DWS INSERT 为 1 个作业
-- ============================================================
BEGIN STATEMENT SET;

-- 1. dws.trade_user_stats（用户交易汇总：下单+支付+退款+优惠券）
INSERT INTO doris_trade_user_stats
SELECT
    user_id,
    CAST(dt AS DATE)         AS dt,
    MAX(user_nick_name)      AS user_nick_name,
    MAX(user_level)          AS user_level,
    MAX(age_range)           AS age_range,
    MAX(gender)              AS gender,
    SUM(order_count)         AS order_count,
    SUM(order_total_amount)  AS order_total_amount,
    SUM(order_original_amount) AS order_original_amount,
    SUM(order_coupon_reduce) AS order_coupon_reduce,
    SUM(payment_count)       AS payment_count,
    SUM(payment_total_amount) AS payment_total_amount,
    SUM(refund_count)        AS refund_count,
    SUM(refund_total_amount) AS refund_total_amount,
    SUM(refund_num)          AS refund_num,
    SUM(coupon_count)        AS coupon_count,
    SUM(coupon_reduce_amount) AS coupon_reduce_amount
FROM (
    SELECT
        user_id, dt,
        user_nick_name, user_level, age_range, gender,
        CAST(1 AS BIGINT)          AS order_count,
        CAST(total_amount AS DECIMAL(18,4))          AS order_total_amount,
        CAST(original_total_amount AS DECIMAL(18,4)) AS order_original_amount,
        CAST(coupon_reduce_amount AS DECIMAL(18,4))  AS order_coupon_reduce,
        CAST(0 AS BIGINT)          AS payment_count,
        CAST(0 AS DECIMAL(18,4))   AS payment_total_amount,
        CAST(0 AS BIGINT)          AS refund_count,
        CAST(0 AS DECIMAL(18,4))   AS refund_total_amount,
        CAST(0 AS INT)             AS refund_num,
        CAST(0 AS BIGINT)          AS coupon_count,
        CAST(0 AS DECIMAL(18,4))   AS coupon_reduce_amount
    FROM paimon.dwd.order_info
    WHERE user_id IS NOT NULL AND dt IS NOT NULL
    UNION ALL
    SELECT
        user_id, dt,
        user_nick_name, user_level, age_range, CAST(NULL AS TINYINT) AS gender,
        CAST(0 AS BIGINT),
        CAST(0 AS DECIMAL(18,4)),
        CAST(0 AS DECIMAL(18,4)),
        CAST(0 AS DECIMAL(18,4)),
        CAST(1 AS BIGINT),
        CAST(total_amount AS DECIMAL(18,4)),
        CAST(0 AS BIGINT),
        CAST(0 AS DECIMAL(18,4)),
        CAST(0 AS INT),
        CAST(0 AS BIGINT),
        CAST(0 AS DECIMAL(18,4))
    FROM paimon.dwd.payment_info
    WHERE user_id IS NOT NULL AND dt IS NOT NULL
    UNION ALL
    SELECT
        user_id, dt,
        user_nick_name, user_level, age_range, CAST(NULL AS TINYINT) AS gender,
        CAST(0 AS BIGINT),
        CAST(0 AS DECIMAL(18,4)),
        CAST(0 AS DECIMAL(18,4)),
        CAST(0 AS DECIMAL(18,4)),
        CAST(0 AS BIGINT),
        CAST(0 AS DECIMAL(18,4)),
        CAST(1 AS BIGINT),
        CAST(refund_amount AS DECIMAL(18,4)),
        refund_num,
        CAST(0 AS BIGINT),
        CAST(0 AS DECIMAL(18,4))
    FROM paimon.dwd.refund_info
    WHERE user_id IS NOT NULL AND dt IS NOT NULL
    UNION ALL
    SELECT
        user_id, dt,
        user_nick_name, user_level, age_range, CAST(NULL AS TINYINT) AS gender,
        CAST(0 AS BIGINT),
        CAST(0 AS DECIMAL(18,4)),
        CAST(0 AS DECIMAL(18,4)),
        CAST(0 AS DECIMAL(18,4)),
        CAST(0 AS BIGINT),
        CAST(0 AS DECIMAL(18,4)),
        CAST(0 AS BIGINT),
        CAST(0 AS DECIMAL(18,4)),
        CAST(0 AS INT),
        CAST(1 AS BIGINT),
        CAST(coupon_reduce_amount AS DECIMAL(18,4))
    FROM paimon.dwd.coupon_use
    WHERE user_id IS NOT NULL AND dt IS NOT NULL
) t
GROUP BY user_id, dt;

-- 2. dws.trade_sku_stats（商品交易汇总：下单明细+加购）
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
    WHERE sku_id IS NOT NULL AND dt IS NOT NULL
    UNION ALL
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
    WHERE sku_id IS NOT NULL AND dt IS NOT NULL
) t
GROUP BY sku_id, dt;

-- 3. dws.log_page_stats（页面流量汇总：PV/UV/停留时长/跳转）
INSERT INTO dws_log_page_stats
SELECT
    page_id,
    CAST(dt AS DATE) AS dt,
    MAX(page_name)           AS page_name,
    COUNT(*)                 AS pv,
    COUNT(DISTINCT user_id)  AS uv,
    SUM(COALESCE(during_time, CAST(0 AS BIGINT))) AS total_during_time,
    SUM(COALESCE(jump_count, 0))                  AS total_jump_count
FROM paimon.dwd.log_page_view
WHERE page_id IS NOT NULL AND dt IS NOT NULL
GROUP BY page_id, dt;

-- 4. dws.log_sku_exposure_stats（商品曝光汇总）
INSERT INTO dws_log_sku_exposure_stats
SELECT
    sku_id,
    CAST(dt AS DATE) AS dt,
    MAX(sku_name)            AS sku_name,
    MAX(spu_name)            AS spu_name,
    MAX(category3_name)      AS category3_name,
    MAX(category2_name)      AS category2_name,
    MAX(category1_name)      AS category1_name,
    MAX(brand_name)          AS brand_name,
    COUNT(*)                 AS exposure_count,
    COUNT(DISTINCT user_id)  AS exposure_user_count
FROM paimon.dwd.log_display
WHERE sku_id IS NOT NULL AND dt IS NOT NULL
GROUP BY sku_id, dt;

-- 5. dws.log_user_action_stats（用户行为汇总：启动+浏览+动作+曝光+错误）
INSERT INTO dws_log_user_action_stats
SELECT
    user_id,
    CAST(dt AS DATE) AS dt,
    MAX(user_nick_name)  AS user_nick_name,
    MAX(user_level)      AS user_level,
    MAX(age_range)       AS age_range,
    MAX(gender)          AS gender,
    SUM(startup_count)    AS startup_count,
    SUM(page_view_count)  AS page_view_count,
    SUM(action_count)     AS action_count,
    SUM(exposure_count)   AS exposure_count,
    SUM(error_count)      AS error_count
FROM (
    SELECT
        user_id, dt, user_nick_name, user_level, age_range, gender,
        CAST(1 AS BIGINT) AS startup_count,
        CAST(0 AS BIGINT) AS page_view_count,
        CAST(0 AS BIGINT) AS action_count,
        CAST(0 AS BIGINT) AS exposure_count,
        CAST(0 AS BIGINT) AS error_count
    FROM paimon.dwd.log_startup
    WHERE user_id IS NOT NULL AND dt IS NOT NULL
    UNION ALL
    SELECT
        user_id, dt, user_nick_name, user_level, age_range, gender,
        CAST(0 AS BIGINT) AS startup_count,
        CAST(1 AS BIGINT) AS page_view_count,
        CAST(0 AS BIGINT) AS action_count,
        CAST(0 AS BIGINT) AS exposure_count,
        CAST(0 AS BIGINT) AS error_count
    FROM paimon.dwd.log_page_view
    WHERE user_id IS NOT NULL AND dt IS NOT NULL
    UNION ALL
    SELECT
        user_id, dt, user_nick_name, user_level, age_range, gender,
        CAST(0 AS BIGINT) AS startup_count,
        CAST(0 AS BIGINT) AS page_view_count,
        CAST(1 AS BIGINT) AS action_count,
        CAST(0 AS BIGINT) AS exposure_count,
        CAST(0 AS BIGINT) AS error_count
    FROM paimon.dwd.log_action
    WHERE user_id IS NOT NULL AND dt IS NOT NULL
    UNION ALL
    SELECT
        user_id, dt, user_nick_name, user_level, age_range, gender,
        CAST(0 AS BIGINT) AS startup_count,
        CAST(0 AS BIGINT) AS page_view_count,
        CAST(0 AS BIGINT) AS action_count,
        CAST(1 AS BIGINT) AS exposure_count,
        CAST(0 AS BIGINT) AS error_count
    FROM paimon.dwd.log_display
    WHERE user_id IS NOT NULL AND dt IS NOT NULL
    UNION ALL
    SELECT
        user_id, dt, user_nick_name, user_level, age_range, gender,
        CAST(0 AS BIGINT) AS startup_count,
        CAST(0 AS BIGINT) AS page_view_count,
        CAST(0 AS BIGINT) AS action_count,
        CAST(0 AS BIGINT) AS exposure_count,
        CAST(1 AS BIGINT) AS error_count
    FROM paimon.dwd.log_error
    WHERE user_id IS NOT NULL AND dt IS NOT NULL
) t
GROUP BY user_id, dt;

END;
