#!/bin/bash
# ============================================================
# Kafka Topic 初始化 - 用户行为日志
# 5 个 topic 对应 5 种日志类型
# ============================================================
# 或在宿主机执行：bash kafka_topics_init.sh
# ============================================================

KAFKA_BIN="/opt/kafka/bin"
BOOTSTRAP="kafka:9092"

# Topic 列表
TOPICS=(
    "ods_log_startup"
    "ods_log_page_view"
    "ods_log_action"
    "ods_log_display"
    "ods_log_error"
)

# 创建 topic
for topic in "${TOPICS[@]}"; do
    echo "Creating topic: $topic"
    docker exec -it kafka ${KAFKA_BIN}/kafka-topics.sh \
        --create \
        --bootstrap-server ${BOOTSTRAP} \
        --topic ${topic} \
        --partitions 3 \
        --replication-factor 1
done

# 查看 topic 列表
echo ""
echo "=== All topics ==="
docker exec -it kafka ${KAFKA_BIN}/kafka-topics.sh \
    --list \
    --bootstrap-server ${BOOTSTRAP}
