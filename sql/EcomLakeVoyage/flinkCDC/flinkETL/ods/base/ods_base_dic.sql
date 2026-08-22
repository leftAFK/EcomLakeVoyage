-- ODS 离线全量同步 - base_dic（字典表）
-- 离线批模式，INSERT OVERWRITE 全量覆盖
-- changelog-producer=none，下游 lookup join
-- docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/EcomLakeVoyage/flinkCDC/flinkETL/ods/base/ods_base_dic.sql

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
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'execution.runtime-mode' = 'batch';

-- ========== Paimon Catalog ==========
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon_warehouse'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ods;

-- ========== MySQL JDBC Source（离线全量读取，bounded）==========
CREATE TEMPORARY TABLE mysql_base_dic (
    id          BIGINT,
    dic_type    STRING,
    code        STRING,
    name        STRING,
    `sort`      INT,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql:3306/gmall_base',
    'username' = 'root',
    'password' = '123456',
    'table-name' = 'base_dic',
    'scan.fetch-size' = '200'
);

-- ========== Paimon Target ==========
CREATE TABLE IF NOT EXISTS ods.base_dic (
    id          BIGINT,
    dic_type    STRING,
    code        STRING,
    name        STRING,
    `sort`      INT,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
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
    'orc.compression' = 'zstd'
);

-- ========== 同步作业 ==========
INSERT OVERWRITE ods.base_dic
SELECT id, dic_type, code, name, `sort`, create_time, update_time
FROM mysql_base_dic;
