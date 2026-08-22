-- ============================================================
-- DWS 日志聚合 - 用户行为汇总（5源 UNION ALL）
-- Flink 读 Paimon DWD 5个日志表 → UNION ALL → 聚合 → 写 Doris
-- 聚合粒度：(user_id, dt)
-- 指标：启动次数、页面浏览次数、动作次数、曝光次数、错误次数
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dws/log/dws_log_user_action_stats.sql
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

-- ========== Doris Sink ==========
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

-- ========== 聚合作业（5源 UNION ALL） ==========
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
    -- 1. 启动日志
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

    -- 2. 页面浏览日志
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

    -- 3. 动作日志
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

    -- 4. 曝光日志（炸裂后的，每行 = 一次 SKU 曝光）
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

    -- 5. 错误日志
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


-- SELECT count(*) FROM dws.log_user_action_stats;