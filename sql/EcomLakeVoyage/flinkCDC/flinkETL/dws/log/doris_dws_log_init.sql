-- ============================================================
-- Doris DWS 日志层建表（无分区，Unique Key 模型）
-- Flink 实时写入
-- ============================================================

CREATE DATABASE IF NOT EXISTS dws;

-- ========== 1. 页面流量汇总 ==========
CREATE TABLE IF NOT EXISTS dws.log_page_stats (
    page_id              VARCHAR(50),
    dt                   DATE,
    page_name            VARCHAR(100),
    pv                   BIGINT,
    uv                   BIGINT,
    total_during_time    BIGINT,
    total_jump_count     INT
)
UNIQUE KEY(page_id, dt)
DISTRIBUTED BY HASH(page_id) BUCKETS 8
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- ========== 2. 用户行为汇总（5源 UNION ALL） ==========
CREATE TABLE IF NOT EXISTS dws.log_user_action_stats (
    user_id              BIGINT,
    dt                   DATE,
    user_nick_name       VARCHAR(100),
    user_level           TINYINT,
    age_range            VARCHAR(20),
    gender               TINYINT,
    startup_count        BIGINT,
    page_view_count      BIGINT,
    action_count         BIGINT,
    exposure_count       BIGINT,
    error_count          BIGINT
)
UNIQUE KEY(user_id, dt)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- ========== 3. SKU 曝光汇总 ==========
CREATE TABLE IF NOT EXISTS dws.log_sku_exposure_stats (
    sku_id               BIGINT,
    dt                   DATE,
    sku_name             VARCHAR(200),
    spu_name             VARCHAR(200),
    category3_name       VARCHAR(100),
    category2_name       VARCHAR(100),
    category1_name       VARCHAR(100),
    brand_name           VARCHAR(100),
    exposure_count       BIGINT,
    exposure_user_count  BIGINT
)
UNIQUE KEY(sku_id, dt)
DISTRIBUTED BY HASH(sku_id) BUCKETS 8
PROPERTIES ("replication_allocation" = "tag.location.default: 1");
