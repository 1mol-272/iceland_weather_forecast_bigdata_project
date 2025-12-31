#!/usr/bin/env bash
# 在 master 上，用 kafka 用户执行：启动 Kafka broker（master）

set -euo pipefail

KAFKA_HOME="/opt/kafka"

cd "$KAFKA_HOME"

echo "== 启动 Kafka broker (master) =="
bin/kafka-server-start.sh -daemon config/server.properties

sleep 2
echo "== 检查 9092 端口 =="
ss -tnlp | grep 9092 || echo "WARN: 似乎没有进程在监听 9092, 请检查日志。"
