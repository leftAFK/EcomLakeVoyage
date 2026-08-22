-- ============================================================
-- DWS 实时汇总 - 用户交易汇总表（Flink 写 Doris）
-- 流模式，UNION ALL 多源聚合
-- 读 Paimon DWD changelog → Flink GROUP BY 增量聚合 → 写 Doris Unique Key 表
--
-- 数据来源：
--   dwd.order_info   → 下单次数、下单金额、优惠券减免
--   dwd.payment_info → 支付次数、支付金额
--   dwd.refund_info  → 退款次数、退款金额、退款件数
--   dwd.coupon_use   → 优惠券领用次数、优惠券减免金额
--
-- 聚合粒度：user_id + dt（每用户每天一行）
-- Doris 表模型：Unique Key（支持 Flink changelog 的 upsert/delete）
--
-- 前置条件：
--   1. dwd.order_info + dwd.payment_info + dwd.refund_info + dwd.coupon_use 必须先执行
--   2. Doris 端 dws.trade_user_stats 表必须先建好（见 doris_init.sql）
--   3. doris-flink-connector JAR 必须放入 /opt/flink/lib/
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dws/business/dws_trade_user_stats.sql
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




-- ========== Doris Sink（临时表，Flink → Doris Stream Load） ==========
-- 注意：fenodes 需替换为你的 Doris FE 地址和 HTTP 端口（默认 8030）
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

-- ========== 汇总作业（UNION ALL 4 源聚合 → Doris） ==========
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
    -- ===== 下单 =====
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

    -- ===== 支付 =====
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

    -- ===== 退款 =====
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

    -- ===== 优惠券领用 =====
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

