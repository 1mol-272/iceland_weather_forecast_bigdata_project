#!/usr/bin/env bash
# 在 master 上，用 kafka 用户执行：启动 Zookeeper

set -euo pipefail

KAFKA_HOME="/opt/kafka"

cd "$KAFKA_HOME"

echo "== 启动 Zookeeper (后台守护） =="

bin/zookeeper-server-start.sh -daemon config/zookeeper.properties

sleep 2
echo "== 检查 2181 端口 =="
ss -tnlp | grep 2181 || echo "WARN: 似乎没有进程在监听 2181, 请检查日志。"
