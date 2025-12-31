#!/usr/bin/env bash
# 在 master 上执行：为 Kafka/ZooKeeper 做一次性目录和配置准备

set -euo pipefail

KAFKA_HOME="/opt/kafka"
KAFKA_USER="kafka"

LOG_DIR="/var/log/kafka_1"
DATA_DIR="/data/kafka/broker_1"
ZK_DATA_DIR="$KAFKA_HOME/zk-data"

if [[ ! -d "$KAFKA_HOME" ]]; then
  echo "ERROR: $KAFKA_HOME 不存在，请确认 Kafka 已经解压到 /opt/kafka 下。"
  exit 1
fi

echo "== 创建日志/数据/ZK目录并设置权限 =="

sudo mkdir -p "$LOG_DIR"
sudo mkdir -p "$DATA_DIR"
sudo mkdir -p "$ZK_DATA_DIR"

sudo chown -R "$KAFKA_USER:$KAFKA_USER" "$LOG_DIR"
sudo chown -R "$KAFKA_USER:$KAFKA_USER" "$DATA_DIR"
sudo chown -R "$KAFKA_USER:$KAFKA_USER" "$ZK_DATA_DIR"

sudo chmod 750 "$LOG_DIR"
sudo chmod 700 "$DATA_DIR"
sudo chmod 700 "$ZK_DATA_DIR"

echo "LOG_DIR=$LOG_DIR"
echo "DATA_DIR=$DATA_DIR"
echo "ZK_DATA_DIR=$ZK_DATA_DIR"

echo "== 配置 server.properties::log.dirs =="
SERVER_PROP="$KAFKA_HOME/config/server.properties"

if grep -q "^log.dirs=" "$SERVER_PROP"; then
  sudo sed -i "s|^log.dirs=.*|log.dirs=$DATA_DIR|" "$SERVER_PROP"
else
  echo "log.dirs=$DATA_DIR" | sudo tee -a "$SERVER_PROP" >/dev/null
fi

echo "== 配置 zookeeper.properties::dataDir =="
ZK_PROP="$KAFKA_HOME/config/zookeeper.properties"

if grep -q "^dataDir=" "$ZK_PROP"; then
  sudo sed -i "s|^dataDir=.*|dataDir=$ZK_DATA_DIR|" "$ZK_PROP"
else
  echo "dataDir=$ZK_DATA_DIR" | sudo tee -a "$ZK_PROP" >/dev/null
fi

echo "Done on master. 之后记得以 kafka 用户启动 zk & broker。"
