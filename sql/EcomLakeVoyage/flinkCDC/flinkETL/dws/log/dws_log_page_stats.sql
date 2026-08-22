-- ============================================================
-- DWS 日志聚合 - 页面流量汇总
-- Flink 读 Paimon DWD log_page_view → 聚合 → 写 Doris
-- 聚合粒度：(page_id, dt)
-- 指标：PV(访问次数)、UV(独立访客数)、总停留时长、总跳转次数
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dws/log/dws_log_page_stats.sql
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

-- ========== 聚合作业 ==========
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


