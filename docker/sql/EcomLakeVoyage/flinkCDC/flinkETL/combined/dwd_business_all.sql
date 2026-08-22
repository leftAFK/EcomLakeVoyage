-- ============================================================
-- DWD 业务宽表合并作业（7 张业务宽表）
-- 用 Statement Set 合并 7 个 INSERT INTO 为 1 个 Flink 作业
-- 占用 slot: 1（常驻流作业）
-- 数据源: Paimon ods.* (业务 7 张)
-- 维表: Paimon dim.* (sku_info/user_info/coupon_info/base_region)
-- 目标: Paimon dwd.* (changelog-producer=input)
-- 依赖: 必须先跑完 ods_cdc_all.sql + dim_streaming_all.sql + dim_batch_all.sql
--   - cart_info        依赖 ods.cart_info + dim.sku_info + dim.user_info
--   - coupon_use       依赖 ods.coupon_use + dim.user_info + dim.coupon_info
--   - order_detail     依赖 ods.order_detail + dim.sku_info + dim.coupon_info
--   - order_info       依赖 ods.order_info + dim.user_info + dim.base_region
--   - order_status_log 依赖 ods.order_status_log (无 join)
--   - payment_info     依赖 ods.payment_info + dim.user_info
--   - refund_info      依赖 ods.refund_info + dim.user_info
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/combined/dwd_business_all.sql
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
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS dwd;

-- ============================================================
-- 1. dwd.cart_info（购物车宽表，拼 sku + user）
-- ============================================================
CREATE TABLE IF NOT EXISTS dwd.cart_info (
    id              BIGINT,
    user_id         BIGINT,
    user_nick_name  STRING,
    user_level      TINYINT,
    age_range       STRING,
    sku_id          BIGINT,
    sku_name        STRING,
    spu_id          BIGINT,
    spu_name        STRING,
    category3_id    BIGINT,
    category3_name  STRING,
    category2_id    BIGINT,
    category2_name  STRING,
    category1_id    BIGINT,
    category1_name  STRING,
    brand_id        BIGINT,
    brand_name      STRING,
    category_id     BIGINT,
    cart_price      DECIMAL(18,2),
    sku_num         INT,
    img_url         STRING,
    sku_attr        STRING,
    order_id        BIGINT,
    is_checked      BOOLEAN,
    create_time     TIMESTAMP(3),
    operate_time    TIMESTAMP(3),
    update_time     TIMESTAMP(3),
    dt              STRING,
    PRIMARY KEY (id, dt) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
    'changelog-producer' = 'input',
    'merge-engine' = 'deduplicate',
    'bucket' = '4',
    'target-file-size' = '128mb',
    'snapshot.time-retained' = '7d',
    'snapshot.num-retained.min' = '10',
    'snapshot.num-retained.max' = '20',
    'changelog.time-retained' = '7d',
    'file.format' = 'orc',
    'orc.compression' = 'zstd',
    'num-sorted-run.compaction-trigger' = '3',
    'compaction.max.file-num' = '5',
    'compaction.max-size-amplification-percent' = '50',
    'full-compaction.delta-commits' = '10'
);
-- ============================================================
-- 2. dwd.coupon_use（优惠券领用宽表，拼 user + coupon）
-- ============================================================
CREATE TABLE IF NOT EXISTS dwd.coupon_use (
    id                   BIGINT,
    coupon_id            BIGINT,
    coupon_name          STRING,
    coupon_type          TINYINT,
    coupon_reduce_amount DECIMAL(18,2),
    full_amount          DECIMAL(18,2),
    reduce_amount        DECIMAL(18,2),
    use_condition        STRING,
    user_id              BIGINT,
    user_nick_name       STRING,
    user_level           TINYINT,
    age_range            STRING,
    order_id             BIGINT,
    coupon_status        SMALLINT,
    get_time             TIMESTAMP(3),
    lock_time            TIMESTAMP(3),
    using_time           TIMESTAMP(3),
    used_time            TIMESTAMP(3),
    expire_time          TIMESTAMP(3),
    update_time          TIMESTAMP(3),
    dt                   STRING,
    PRIMARY KEY (id, dt) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
    'changelog-producer' = 'input',
    'merge-engine' = 'deduplicate',
    'bucket' = '4',
    'target-file-size' = '128mb',
    'snapshot.time-retained' = '7d',
    'snapshot.num-retained.min' = '10',
    'snapshot.num-retained.max' = '20',
    'changelog.time-retained' = '7d',
    'file.format' = 'orc',
    'orc.compression' = 'zstd',
    'num-sorted-run.compaction-trigger' = '3',
    'compaction.max.file-num' = '5',
    'compaction.max-size-amplification-percent' = '50',
    'full-compaction.delta-commits' = '10'
);
-- ============================================================
-- 3. dwd.order_detail（订单明细宽表，拼 sku + coupon）
-- ============================================================
CREATE TABLE IF NOT EXISTS dwd.order_detail (
    id                    BIGINT,
    order_id              BIGINT,
    order_line_no         INT,
    sku_id                BIGINT,
    sku_name              STRING,
    spu_id                BIGINT,
    spu_name              STRING,
    category3_id          BIGINT,
    category3_name        STRING,
    category2_id          BIGINT,
    category2_name        STRING,
    category1_id          BIGINT,
    category1_name        STRING,
    brand_id              BIGINT,
    brand_name            STRING,
    img_url               STRING,
    order_price           DECIMAL(18,2),
    sku_num               INT,
    create_time           TIMESTAMP(3),
    source_type           SMALLINT,
    source_id             BIGINT,
    split_activity_amount DECIMAL(18,4),
    coupon_id             BIGINT,
    coupon_name           STRING,
    coupon_type           TINYINT,
    split_coupon_amount   DECIMAL(18,4),
    split_total_amount    DECIMAL(18,4),
    update_time           TIMESTAMP(3),
    dt                    STRING,
    PRIMARY KEY (id, dt) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
    'changelog-producer' = 'input',
    'merge-engine' = 'deduplicate',
    'bucket' = '4',
    'target-file-size' = '128mb',
    'snapshot.time-retained' = '7d',
    'snapshot.num-retained.min' = '10',
    'snapshot.num-retained.max' = '20',
    'changelog.time-retained' = '7d',
    'file.format' = 'orc',
    'orc.compression' = 'zstd',
    'num-sorted-run.compaction-trigger' = '3',
    'compaction.max.file-num' = '5',
    'compaction.max-size-amplification-percent' = '50',
    'full-compaction.delta-commits' = '10'
);
-- ============================================================
-- 4. dwd.order_info（订单宽表，拼 user + region）
-- ============================================================
CREATE TABLE IF NOT EXISTS dwd.order_info (
    id                    BIGINT,
    consignee             STRING,
    consignee_tel         STRING,
    total_amount          DECIMAL(18,2),
    order_status          SMALLINT,
    user_id               BIGINT,
    user_nick_name        STRING,
    user_level            TINYINT,
    age_range             STRING,
    gender                TINYINT,
    payment_way           TINYINT,
    delivery_address      STRING,
    order_comment         STRING,
    out_trade_no          STRING,
    trade_body            STRING,
    create_time           TIMESTAMP(3),
    operate_time          TIMESTAMP(3),
    receive_time          TIMESTAMP(3),
    expire_time           TIMESTAMP(3),
    province_id           INT,
    region_name           STRING,
    big_region            STRING,
    coupon_reduce_amount  DECIMAL(18,2),
    original_total_amount DECIMAL(18,2),
    update_time           TIMESTAMP(3),
    dt                    STRING,
    PRIMARY KEY (id, dt) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
    'changelog-producer' = 'input',
    'merge-engine' = 'deduplicate',
    'bucket' = '4',
    'target-file-size' = '128mb',
    'snapshot.time-retained' = '7d',
    'snapshot.num-retained.min' = '10',
    'snapshot.num-retained.max' = '20',
    'changelog.time-retained' = '7d',
    'file.format' = 'orc',
    'orc.compression' = 'zstd',
    'num-sorted-run.compaction-trigger' = '3',
    'compaction.max.file-num' = '5',
    'compaction.max-size-amplification-percent' = '50',
    'full-compaction.delta-commits' = '10'
);
-- ============================================================
-- 5. dwd.order_status_log（订单状态履历，无 join，直传）
-- ============================================================
CREATE TABLE IF NOT EXISTS dwd.order_status_log (
    id           BIGINT,
    order_id     BIGINT,
    order_status SMALLINT,
    create_time  TIMESTAMP(3),
    dt           STRING
) PARTITIONED BY (dt) WITH (
    'bucket' = '4',
    'bucket-key' = 'id',
    'target-file-size' = '128mb',
    'snapshot.time-retained' = '7d',
    'snapshot.num-retained.min' = '10',
    'snapshot.num-retained.max' = '20',
    'file.format' = 'orc',
    'orc.compression' = 'zstd',
    'num-sorted-run.compaction-trigger' = '3',
    'compaction.max.file-num' = '5',
    'compaction.max-size-amplification-percent' = '50',
    'full-compaction.delta-commits' = '10'
);
-- ============================================================
-- 6. dwd.payment_info（支付宽表，拼 user）
-- ============================================================
CREATE TABLE IF NOT EXISTS dwd.payment_info (
    id                BIGINT,
    out_trade_no      STRING,
    order_id          BIGINT,
    user_id           BIGINT,
    user_nick_name    STRING,
    user_level        TINYINT,
    age_range         STRING,
    payment_type      TINYINT,
    trade_no          STRING,
    total_amount      DECIMAL(18,2),
    payment_status    SMALLINT,
    create_time       TIMESTAMP(3),
    callback_time     TIMESTAMP(3),
    callback_content  STRING,
    update_time       TIMESTAMP(3),
    dt                STRING,
    PRIMARY KEY (id, dt) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
    'changelog-producer' = 'input',
    'merge-engine' = 'deduplicate',
    'bucket' = '4',
    'target-file-size' = '128mb',
    'snapshot.time-retained' = '7d',
    'snapshot.num-retained.min' = '10',
    'snapshot.num-retained.max' = '20',
    'changelog.time-retained' = '7d',
    'file.format' = 'orc',
    'orc.compression' = 'zstd',
    'num-sorted-run.compaction-trigger' = '3',
    'compaction.max.file-num' = '5',
    'compaction.max-size-amplification-percent' = '50',
    'full-compaction.delta-commits' = '10'
);
-- ============================================================
-- 7. dwd.refund_info（退款宽表，拼 user）
-- ============================================================
CREATE TABLE IF NOT EXISTS dwd.refund_info (
    id                  BIGINT,
    user_id             BIGINT,
    user_nick_name      STRING,
    user_level          TINYINT,
    age_range           STRING,
    order_id            BIGINT,
    order_detail_id     BIGINT,
    sku_name            STRING,
    refund_amount       DECIMAL(18,2),
    refund_num          INT,
    refund_status       SMALLINT,
    refund_type         TINYINT,
    refund_reason       STRING,
    refund_reason_type  TINYINT,
    create_time         TIMESTAMP(3),
    refund_time         TIMESTAMP(3),
    operate_time        TIMESTAMP(3),
    update_time         TIMESTAMP(3),
    dt                  STRING,
    PRIMARY KEY (id, dt) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
    'changelog-producer' = 'input',
    'merge-engine' = 'deduplicate',
    'bucket' = '4',
    'target-file-size' = '128mb',
    'snapshot.time-retained' = '7d',
    'snapshot.num-retained.min' = '10',
    'snapshot.num-retained.max' = '20',
    'changelog.time-retained' = '7d',
    'file.format' = 'orc',
    'orc.compression' = 'zstd',
    'num-sorted-run.compaction-trigger' = '3',
    'compaction.max.file-num' = '5',
    'compaction.max-size-amplification-percent' = '50',
    'full-compaction.delta-commits' = '10'
);
-- ============================================================
-- Statement Set：合并 7 个 INSERT INTO 为 1 个作业
-- ============================================================
BEGIN STATEMENT SET;

-- 1. cart_info
INSERT INTO dwd.cart_info
SELECT
    c.id,
    c.user_id,
    u.nick_name         AS user_nick_name,
    u.user_level,
    u.age_range,
    c.sku_id,
    c.sku_name,
    sku.spu_id,
    sku.spu_name,
    sku.category3_id,
    sku.category3_name,
    sku.category2_id,
    sku.category2_name,
    sku.category1_id,
    sku.category1_name,
    sku.brand_id,
    sku.brand_name,
    c.category_id,
    c.cart_price,
    c.sku_num,
    c.img_url,
    c.sku_attr,
    c.order_id,
    c.is_checked,
    c.create_time,
    c.operate_time,
    c.update_time,
    c.dt
FROM (
    SELECT *, PROCTIME() AS proctime
    FROM ods.cart_info
) c
LEFT JOIN dim.sku_info FOR SYSTEM_TIME AS OF c.proctime AS sku
    ON c.sku_id = sku.id
LEFT JOIN dim.user_info FOR SYSTEM_TIME AS OF c.proctime AS u
    ON c.user_id = u.id;

-- 2. coupon_use
INSERT INTO dwd.coupon_use
SELECT
    cu.id,
    cu.coupon_id,
    cou.coupon_name,
    cu.coupon_type,
    cu.coupon_reduce_amount,
    cou.full_amount,
    cou.reduce_amount,
    cou.use_condition,
    cu.user_id,
    u.nick_name         AS user_nick_name,
    u.user_level,
    u.age_range,
    cu.order_id,
    cu.coupon_status,
    cu.get_time,
    cu.lock_time,
    cu.using_time,
    cu.used_time,
    cu.expire_time,
    cu.update_time,
    cu.dt
FROM (
    SELECT *, PROCTIME() AS proctime
    FROM ods.coupon_use
) cu
LEFT JOIN dim.user_info FOR SYSTEM_TIME AS OF cu.proctime AS u
    ON cu.user_id = u.id
LEFT JOIN dim.coupon_info FOR SYSTEM_TIME AS OF cu.proctime AS cou
    ON cu.coupon_id = cou.id;

-- 3. order_detail
INSERT INTO dwd.order_detail
SELECT
    od.id,
    od.order_id,
    od.order_line_no,
    od.sku_id,
    od.sku_name,
    sku.spu_id,
    sku.spu_name,
    sku.category3_id,
    sku.category3_name,
    sku.category2_id,
    sku.category2_name,
    sku.category1_id,
    sku.category1_name,
    sku.brand_id,
    sku.brand_name,
    od.img_url,
    od.order_price,
    od.sku_num,
    od.create_time,
    od.source_type,
    od.source_id,
    od.split_activity_amount,
    od.coupon_id,
    cou.coupon_name,
    cou.coupon_type,
    od.split_coupon_amount,
    od.split_total_amount,
    od.update_time,
    od.dt
FROM (
    SELECT *, PROCTIME() AS proctime
    FROM ods.order_detail
) od
LEFT JOIN dim.sku_info FOR SYSTEM_TIME AS OF od.proctime AS sku
    ON od.sku_id = sku.id
LEFT JOIN dim.coupon_info FOR SYSTEM_TIME AS OF od.proctime AS cou
    ON od.coupon_id = cou.id;

-- 4. order_info
INSERT INTO dwd.order_info
SELECT
    o.id,
    o.consignee,
    o.consignee_tel,
    o.total_amount,
    o.order_status,
    o.user_id,
    u.nick_name         AS user_nick_name,
    u.user_level,
    u.age_range,
    u.gender,
    o.payment_way,
    o.delivery_address,
    o.order_comment,
    o.out_trade_no,
    o.trade_body,
    o.create_time,
    o.operate_time,
    o.receive_time,
    o.expire_time,
    o.province_id,
    r.region_name,
    r.big_region,
    o.coupon_reduce_amount,
    o.original_total_amount,
    o.update_time,
    o.dt
FROM (
    SELECT *,
           CAST(province_id AS BIGINT) AS region_id,
           PROCTIME() AS proctime
    FROM ods.order_info
) o
LEFT JOIN dim.user_info FOR SYSTEM_TIME AS OF o.proctime AS u
    ON o.user_id = u.id
LEFT JOIN dim.base_region FOR SYSTEM_TIME AS OF o.proctime AS r
    ON o.region_id = r.id;

-- 5. order_status_log（无 join，直传）
INSERT INTO dwd.order_status_log
SELECT id, order_id, order_status, create_time, dt
FROM ods.order_status_log;

-- 6. payment_info
INSERT INTO dwd.payment_info
SELECT
    p.id,
    p.out_trade_no,
    p.order_id,
    p.user_id,
    u.nick_name         AS user_nick_name,
    u.user_level,
    u.age_range,
    p.payment_type,
    p.trade_no,
    p.total_amount,
    p.payment_status,
    p.create_time,
    p.callback_time,
    p.callback_content,
    p.update_time,
    p.dt
FROM (
    SELECT *, PROCTIME() AS proctime
    FROM ods.payment_info
) p
LEFT JOIN dim.user_info FOR SYSTEM_TIME AS OF p.proctime AS u
    ON p.user_id = u.id;

-- 7. refund_info
INSERT INTO dwd.refund_info
SELECT
    r.id,
    r.user_id,
    u.nick_name         AS user_nick_name,
    u.user_level,
    u.age_range,
    r.order_id,
    r.order_detail_id,
    r.sku_name,
    r.refund_amount,
    r.refund_num,
    r.refund_status,
    r.refund_type,
    r.refund_reason,
    r.refund_reason_type,
    r.create_time,
    r.refund_time,
    r.operate_time,
    r.update_time,
    r.dt
FROM (
    SELECT *, PROCTIME() AS proctime
    FROM ods.refund_info
) r
LEFT JOIN dim.user_info FOR SYSTEM_TIME AS OF r.proctime AS u
    ON r.user_id = u.id;

END;
