#!/bin/bash
# ============================================================
# 启动 Hadoop (HDFS + YARN) + Hive (Metastore + HiveServer2)
# ============================================================

set -e

echo "=========================================="
echo "  [1/3] 启动 Hadoop (HDFS + YARN)"
echo "=========================================="
mkdir -p /Users/ok/bigdata/hadoop/data/namenode
mkdir -p /Users/ok/bigdata/hadoop/data/datanode
mkdir -p /Users/ok/bigdata/hadoop/tmp

if [ ! -f /Users/ok/bigdata/hadoop/data/namenode/current/VERSION ]; then
  echo "首次启动，格式化 NameNode..."
  hdfs namenode -format -force
fi

start-dfs.sh
sleep 3
start-yarn.sh
sleep 3

echo "等待 HDFS 就绪..."
for i in {1..30}; do
  if hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; then
    echo "HDFS 已就绪"
    break
  fi
  echo "  等待安全模式解除... ($i/30)"
  sleep 2
done

hdfs dfs -mkdir -p /user/hive/warehouse 2>/dev/null
hdfs dfs -mkdir -p /iceberg_warehouse 2>/dev/null
hdfs dfs -chmod -R 777 /user/hive/warehouse 2>/dev/null
hdfs dfs -chmod -R 777 /iceberg_warehouse 2>/dev/null

echo ""

echo "=========================================="
echo "  [2/3] 启动 Hive Metastore"
echo "=========================================="
pkill -f HiveMetaStore 2>/dev/null || true
pkill -f hiveserver2 2>/dev/null || true
sleep 2

nohup hive --service metastore > /Users/ok/bigdata/hive/logs/metastore.log 2>&1 &
echo "Hive Metastore PID: $!"

echo "等待 Metastore 就绪..."
for i in {1..30}; do
  if lsof -i:9083 2>/dev/null | grep -q LISTEN; then
    echo "Metastore 已就绪 (9083)"
    break
  fi
  echo "  等待 Metastore 启动... ($i/30)"
  sleep 2
done

echo ""

echo "=========================================="
echo "  [3/3] 启动 HiveServer2"
echo "=========================================="
nohup hive --service hiveserver2 > /Users/ok/bigdata/hive/logs/hiveserver2.log 2>&1 &
echo "HiveServer2 PID: $!"

echo "等待 HiveServer2 就绪..."
for i in {1..60}; do
  if lsof -i:10000 2>/dev/null | grep -q LISTEN; then
    echo "HiveServer2 已就绪 (10000)"
    break
  fi
  echo "  等待 HiveServer2 启动... ($i/60)"
  sleep 2
done

echo ""
echo "=========================================="
echo "  启动完成!"
echo "=========================================="
echo ""
echo "服务地址:"
echo "  HDFS NameNode:    http://localhost:9870"
echo "  YARN ResourceMgr: http://localhost:8088"
echo ""
echo "端口检查:"
echo "  HDFS:       $(lsof -i:9000 2>/dev/null | grep LISTEN | wc -l | tr -d ' ') listener(s)"
echo "  Metastore:  $(lsof -i:9083 2>/dev/null | grep LISTEN | wc -l | tr -d ' ') listener(s)"
echo "  HiveServer2:$(lsof -i:10000 2>/dev/null | grep LISTEN | wc -l | tr -d ' ') listener(s)"
echo ""
echo "Hadoop 进程:"
jps 2>/dev/null | grep -E "NameNode|DataNode|SecondaryNameNode|ResourceManager|NodeManager" || echo "  无 Hadoop Java 进程"
