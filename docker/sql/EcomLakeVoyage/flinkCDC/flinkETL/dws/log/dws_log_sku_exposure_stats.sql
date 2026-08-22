-- ============================================================
-- DWS 日志聚合 - SKU 曝光汇总
-- Flink 读 Paimon DWD log_display（炸裂后）→ 聚合 → 写 Doris
-- 聚合粒度：(sku_id, dt)
-- 指标：曝光次数、曝光用户数
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/dws/log/dws_log_sku_exposure_stats.sql
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

-- ========== 聚合作业 ==========
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

