#!/usr/bin/env bash
# 在 master 上，以 hadoop 用户运行

sudo -i -u hadoop bash -lc "
  source ~/.bashrc
  hdfs --daemon start namenode
  hdfs --daemon start datanode
  start-yarn.sh
  jps
  ssh worker1 'jps'
  hdfs dfs -mkdir -p /data/weather/raw
  hdfs dfs -mkdir -p /checkpoints/weather_raw
"