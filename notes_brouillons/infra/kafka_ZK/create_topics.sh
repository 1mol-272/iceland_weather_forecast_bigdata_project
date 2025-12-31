#!/usr/bin/env bash
# 在 master 上，以 kafka 用户运行: ：创建 weather_iceland_raw topic

set -euo pipefail

KAFKA_HOME="/opt/kafka"

cd "$KAFKA_HOME"

TOPIC="weather_iceland_raw"

bin/kafka-topics.sh --create \
  --topic "$TOPIC" \
  --bootstrap-server master:9092 \
  --partitions 3 \
  --replication-factor 1

echo "== 描述 topic =="
bin/kafka-topics.sh --describe \
  --topic "$TOPIC" \
  --bootstrap-server master:9092

