-- ============================================================
-- ODS CDC 实时同步合并作业（11 张 MySQL CDC 表）
-- 用 Statement Set 合并 11 个 INSERT INTO 为 1 个 Flink 作业
-- 占用 slot: 1（常驻流作业）
-- 数据源: MySQL CDC (gmall_base 3 张 + gmall_business 7 张 + base 的 coupon_info)
--   - base: coupon_info, sku_info, spu_info, user_info
--   - business: cart_info, coupon_use, order_detail, order_info,
--               order_status_log, payment_info, refund_info
-- 目标: Paimon ods.* (changelog-producer=input, 完整 changelog)
-- 注意: server-id 全部独立分配 (5444~5564)，无冲突
-- ============================================================
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/combined/ods_cdc_all.sql
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
SET 'table.exec.state.ttl' = '0s';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'execution.runtime-mode' = 'streaming';

-- ========== Paimon Catalog ==========
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon_warehouse'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ods;

-- ============================================================
-- 1. coupon_info（优惠券模板表）server-id=5444-5454
-- ============================================================
CREATE TEMPORARY TABLE mysql_coupon_info (
    id            BIGINT,
    coupon_type   TINYINT,
    full_amount   DECIMAL(18,2),
    reduce_amount DECIMAL(18,2),
    coupon_name   STRING,
    use_condition STRING,
    create_time   TIMESTAMP(3),
    update_time   TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'gmall_base',
    'table-name' = 'coupon_info',
    'server-id' = '5444-5454',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE IF NOT EXISTS ods.coupon_info (
    id            BIGINT,
    coupon_type   TINYINT,
    full_amount   DECIMAL(18,2),
    reduce_amount DECIMAL(18,2),
    coupon_name   STRING,
    use_condition STRING,
    create_time   TIMESTAMP(3),
    update_time   TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'changelog-producer' = 'none',
    'merge-engine' = 'deduplicate',
    'bucket' = '1',
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
-- 2. sku_info（SKU 商品表）server-id=5466-5476
-- ============================================================
CREATE TEMPORARY TABLE mysql_sku_info (
    id           BIGINT,
    sku_name     STRING,
    spu_id       BIGINT,
    category3_id BIGINT,
    brand_id     BIGINT,
    price        DECIMAL(18,2),
    weight       DECIMAL(10,2),
    img_url      STRING,
    is_sale      TINYINT,
    sku_attr     STRING,
    create_time  TIMESTAMP(3),
    update_time  TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'gmall_base',
    'table-name' = 'sku_info',
    'server-id' = '5466-5476',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE IF NOT EXISTS ods.sku_info (
    id           BIGINT,
    sku_name     STRING,
    spu_id       BIGINT,
    category3_id BIGINT,
    brand_id     BIGINT,
    price        DECIMAL(18,2),
    weight       DECIMAL(10,2),
    img_url      STRING,
    is_sale      TINYINT,
    sku_attr     STRING,
    create_time  TIMESTAMP(3),
    update_time  TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
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
-- 3. spu_info（SPU 商品表）server-id=5455-5465
-- ============================================================
CREATE TEMPORARY TABLE mysql_spu_info (
    id           BIGINT,
    spu_name     STRING,
    description  STRING,
    category3_id BIGINT,
    brand_id     BIGINT,
    create_time  TIMESTAMP(3),
    update_time  TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'gmall_base',
    'table-name' = 'spu_info',
    'server-id' = '5455-5465',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE IF NOT EXISTS ods.spu_info (
    id           BIGINT,
    spu_name     STRING,
    description  STRING,
    category3_id BIGINT,
    brand_id     BIGINT,
    create_time  TIMESTAMP(3),
    update_time  TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'changelog-producer' = 'none',
    'merge-engine' = 'deduplicate',
    'bucket' = '1',
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
-- 4. user_info（用户表）server-id=5477-5487
-- ============================================================
CREATE TEMPORARY TABLE mysql_user_info (
    id          BIGINT,
    login_name  STRING,
    nick_name   STRING,
    name        STRING,
    phone_num   STRING,
    email       STRING,
    user_level  TINYINT,
    birthday    DATE,
    gender      TINYINT,
    age_range   STRING,
    status      SMALLINT,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'gmall_base',
    'table-name' = 'user_info',
    'server-id' = '5477-5487',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE IF NOT EXISTS ods.user_info (
    id          BIGINT,
    login_name  STRING,
    nick_name   STRING,
    name        STRING,
    phone_num   STRING,
    email       STRING,
    user_level  TINYINT,
    birthday    DATE,
    gender      TINYINT,
    age_range   STRING,
    status      SMALLINT,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
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
-- 5. cart_info（购物车表）server-id=5521-5531
-- ============================================================
CREATE TEMPORARY TABLE mysql_cart_info (
    id           BIGINT,
    user_id      BIGINT,
    sku_id       BIGINT,
    sku_name     STRING,
    category_id  BIGINT,
    cart_price   DECIMAL(18,2),
    sku_num      INT,
    img_url      STRING,
    sku_attr     STRING,
    order_id     BIGINT,
    is_checked   BOOLEAN,
    create_time  TIMESTAMP(3),
    operate_time TIMESTAMP(3),
    update_time  TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'gmall_business',
    'table-name' = 'cart_info',
    'server-id' = '5521-5531',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE IF NOT EXISTS ods.cart_info (
    id           BIGINT,
    user_id      BIGINT,
    sku_id       BIGINT,
    sku_name     STRING,
    category_id  BIGINT,
    cart_price   DECIMAL(18,2),
    sku_num      INT,
    img_url      STRING,
    sku_attr     STRING,
    order_id     BIGINT,
    is_checked   BOOLEAN,
    create_time  TIMESTAMP(3),
    operate_time TIMESTAMP(3),
    update_time  TIMESTAMP(3),
    dt           STRING,
    WATERMARK FOR create_time AS create_time - INTERVAL '5' SECOND,
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
-- 6. coupon_use（优惠券领用表）server-id=5554-5564
-- ============================================================
CREATE TEMPORARY TABLE mysql_coupon_use (
    id                  BIGINT,
    coupon_id           BIGINT,
    coupon_type         TINYINT,
    user_id             BIGINT,
    order_id            BIGINT,
    coupon_status       SMALLINT,
    coupon_reduce_amount DECIMAL(18,2),
    get_time            TIMESTAMP(3),
    lock_time           TIMESTAMP(3),
    using_time          TIMESTAMP(3),
    used_time           TIMESTAMP(3),
    expire_time         TIMESTAMP(3),
    update_time         TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'gmall_business',
    'table-name' = 'coupon_use',
    'server-id' = '5554-5564',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE IF NOT EXISTS ods.coupon_use (
    id                  BIGINT,
    coupon_id           BIGINT,
    coupon_type         TINYINT,
    user_id             BIGINT,
    order_id            BIGINT,
    coupon_status       SMALLINT,
    coupon_reduce_amount DECIMAL(18,2),
    get_time            TIMESTAMP(3),
    lock_time           TIMESTAMP(3),
    using_time          TIMESTAMP(3),
    used_time           TIMESTAMP(3),
    expire_time         TIMESTAMP(3),
    update_time         TIMESTAMP(3),
    dt                  STRING,
    WATERMARK FOR get_time AS get_time - INTERVAL '5' SECOND,
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
-- 7. order_detail（订单明细表）server-id=5510-5520
-- ============================================================
CREATE TEMPORARY TABLE mysql_order_detail (
    id                    BIGINT,
    order_id              BIGINT,
    order_line_no         INT,
    sku_id                BIGINT,
    sku_name              STRING,
    img_url               STRING,
    order_price           DECIMAL(18,2),
    sku_num               INT,
    create_time           TIMESTAMP(3),
    source_type           SMALLINT,
    source_id             BIGINT,
    split_activity_amount DECIMAL(18,4),
    coupon_id             BIGINT,
    split_coupon_amount   DECIMAL(18,4),
    split_total_amount    DECIMAL(18,4),
    update_time           TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'gmall_business',
    'table-name' = 'order_detail',
    'server-id' = '5510-5520',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE IF NOT EXISTS ods.order_detail (
    id                    BIGINT,
    order_id              BIGINT,
    order_line_no         INT,
    sku_id                BIGINT,
    sku_name              STRING,
    img_url               STRING,
    order_price           DECIMAL(18,2),
    sku_num               INT,
    create_time           TIMESTAMP(3),
    source_type           SMALLINT,
    source_id             BIGINT,
    split_activity_amount DECIMAL(18,4),
    coupon_id             BIGINT,
    split_coupon_amount   DECIMAL(18,4),
    split_total_amount    DECIMAL(18,4),
    update_time           TIMESTAMP(3),
    dt                    STRING,
    WATERMARK FOR create_time AS create_time - INTERVAL '5' SECOND,
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
-- 8. order_info（订单表）server-id=5488-5498
-- ============================================================
CREATE TEMPORARY TABLE mysql_order_info (
    id                    BIGINT,
    consignee             STRING,
    consignee_tel         STRING,
    total_amount          DECIMAL(18,2),
    order_status          SMALLINT,
    user_id               BIGINT,
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
    coupon_reduce_amount  DECIMAL(18,2),
    original_total_amount DECIMAL(18,2),
    update_time           TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'gmall_business',
    'table-name' = 'order_info',
    'server-id' = '5488-5498',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE IF NOT EXISTS ods.order_info (
    id                    BIGINT,
    consignee             STRING,
    consignee_tel         STRING,
    total_amount          DECIMAL(18,2),
    order_status          SMALLINT,
    user_id               BIGINT,
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
    coupon_reduce_amount  DECIMAL(18,2),
    original_total_amount DECIMAL(18,2),
    update_time           TIMESTAMP(3),
    dt                    STRING,
    WATERMARK FOR create_time AS create_time - INTERVAL '5' SECOND,
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
-- 9. order_status_log（订单状态履历表）server-id=5499-5509
-- ============================================================
CREATE TEMPORARY TABLE mysql_order_status_log (
    id           BIGINT,
    order_id     BIGINT,
    order_status SMALLINT,
    create_time  TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'gmall_business',
    'table-name' = 'order_status_log',
    'server-id' = '5499-5509',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE IF NOT EXISTS ods.order_status_log (
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
-- 10. payment_info（支付表）server-id=5532-5542
-- ============================================================
CREATE TEMPORARY TABLE mysql_payment_info (
    id                BIGINT,
    out_trade_no      STRING,
    order_id          BIGINT,
    user_id           BIGINT,
    payment_type      TINYINT,
    trade_no          STRING,
    total_amount      DECIMAL(18,2),
    payment_status    SMALLINT,
    create_time       TIMESTAMP(3),
    callback_time     TIMESTAMP(3),
    callback_content  STRING,
    update_time       TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'gmall_business',
    'table-name' = 'payment_info',
    'server-id' = '5532-5542',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE IF NOT EXISTS ods.payment_info (
    id                BIGINT,
    out_trade_no      STRING,
    order_id          BIGINT,
    user_id           BIGINT,
    payment_type      TINYINT,
    trade_no          STRING,
    total_amount      DECIMAL(18,2),
    payment_status    SMALLINT,
    create_time       TIMESTAMP(3),
    callback_time     TIMESTAMP(3),
    callback_content  STRING,
    update_time       TIMESTAMP(3),
    dt                STRING,
    WATERMARK FOR create_time AS create_time - INTERVAL '5' SECOND,
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
-- 11. refund_info（退款表）server-id=5543-5553
-- ============================================================
CREATE TEMPORARY TABLE mysql_refund_info (
    id                  BIGINT,
    user_id             BIGINT,
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
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'gmall_business',
    'table-name' = 'refund_info',
    'server-id' = '5543-5553',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE IF NOT EXISTS ods.refund_info (
    id                  BIGINT,
    user_id             BIGINT,
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
    WATERMARK FOR create_time AS create_time - INTERVAL '5' SECOND,
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
-- Statement Set：合并 11 个 CDC INSERT 为 1 个作业
-- ============================================================
BEGIN STATEMENT SET;

INSERT INTO ods.coupon_info
SELECT id, coupon_type, full_amount, reduce_amount, coupon_name,
       use_condition, create_time, update_time
FROM mysql_coupon_info;

INSERT INTO ods.sku_info
SELECT id, sku_name, spu_id, category3_id, brand_id, price, weight,
       img_url, is_sale, sku_attr, create_time, update_time
FROM mysql_sku_info;

INSERT INTO ods.spu_info
SELECT id, spu_name, description, category3_id, brand_id, create_time, update_time
FROM mysql_spu_info;

INSERT INTO ods.user_info
SELECT id, login_name, nick_name, name, phone_num, email, user_level,
       birthday, gender, age_range, status, create_time, update_time
FROM mysql_user_info;

INSERT INTO ods.cart_info
SELECT id, user_id, sku_id, sku_name, category_id, cart_price,
       sku_num, img_url, sku_attr, order_id, is_checked,
       create_time, operate_time, update_time,
       DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt
FROM mysql_cart_info;

INSERT INTO ods.coupon_use
SELECT id, coupon_id, coupon_type, user_id, order_id, coupon_status,
       coupon_reduce_amount, get_time, lock_time, using_time,
       used_time, expire_time, update_time,
       DATE_FORMAT(get_time, 'yyyy-MM-dd') AS dt
FROM mysql_coupon_use;

INSERT INTO ods.order_detail
SELECT id, order_id, order_line_no, sku_id, sku_name, img_url,
       order_price, sku_num, create_time, source_type, source_id,
       split_activity_amount, coupon_id, split_coupon_amount, split_total_amount,
       update_time,
       DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt
FROM mysql_order_detail;

INSERT INTO ods.order_info
SELECT id, consignee, consignee_tel, total_amount, order_status, user_id,
       payment_way, delivery_address, order_comment, out_trade_no, trade_body,
       create_time, operate_time, receive_time, expire_time, province_id,
       coupon_reduce_amount, original_total_amount, update_time,
       DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt
FROM mysql_order_info;

INSERT INTO ods.order_status_log
SELECT id, order_id, order_status, create_time,
       DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt
FROM mysql_order_status_log;

INSERT INTO ods.payment_info
SELECT id, out_trade_no, order_id, user_id, payment_type,
       trade_no, total_amount, payment_status,
       create_time, callback_time, callback_content, update_time,
       DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt
FROM mysql_payment_info;

INSERT INTO ods.refund_info
SELECT id, user_id, order_id, order_detail_id, sku_name,
       refund_amount, refund_num, refund_status, refund_type,
       refund_reason, refund_reason_type,
       create_time, refund_time, operate_time, update_time,
       DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt
FROM mysql_refund_info;

END;
