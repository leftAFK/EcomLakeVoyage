
-- ODS CDC 实时同步 - user_info（用户表）
-- 流模式，INSERT INTO 持续同步
-- changelog-producer=input，产完整 changelog 供下游 temporal join

-- 
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/base/ods_user_info.sql

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
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'execution.runtime-mode' = 'streaming';

-- ========== Paimon Catalog ==========
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon_warehouse'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ods;

-- ========== MySQL CDC Source ==========
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
    -- 'scan.startup.mode' = 'initial'
    'scan.startup.mode' = 'latest-offset'
    
);

-- ========== Paimon Target ==========
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
    'orc.compression' = 'zstd'
);

-- ========== 同步作业 ==========
INSERT INTO ods.user_info
SELECT id, login_name, nick_name, name, phone_num, email, user_level,
       birthday, gender, age_range, status, create_time, update_time
FROM mysql_user_info;
