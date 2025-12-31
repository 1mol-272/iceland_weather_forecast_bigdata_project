#!/usr/bin/env bash
# 在 worker1 上，以 hadoop 用户运行

sudo -i -u hadoop bash -lc "
  source ~/.bashrc
  hdfs --daemon start datanode
  jps
"
