#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_load_env.sh"

usage() {
  cat <<EOF
Usage: ./scripts/pipeline.sh <command>

Commands:
  start     Start HDFS+YARN, Kafka(ZK+broker), Producer, Spark streaming (YARN)
  stop      Stop Producer, kill Spark app on YARN, stop Kafka, stop YARN/HDFS
  status    Show NodeManagers, Spark apps, Kafka topic describe, HDFS output size
  tunnel    SSH tunnel for YARN RM UI: http://localhost:8088
EOF
}

start_all() {
  echo "[0/5] Apply Hadoop configs"
  run_master "
    set -e
    for f in core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml; do
      src='$HADOOP_CFG_REPO_DIR/'\"\$f\"
      dst='$HADOOP_CONF_DIR/'\"\$f\"
      [ -f \"\$src\" ] || { echo \"MISSING \$src\"; exit 1; }
      cp -f \"\$src\" \"\$dst\" 2>/dev/null || sudo cp -f \"\$src\" \"\$dst\"
    done
  "

  echo "[0.5/5] Apply Kafka configs on master"
  run_master "
    test -f '$KAFKA_CFG_REPO_DIR/server.properties' || { echo 'MISSING $KAFKA_CFG_REPO_DIR/server.properties'; exit 1; }
    test -f '$KAFKA_CFG_REPO_DIR/zookeeper.properties' || { echo 'MISSING $KAFKA_CFG_REPO_DIR/zookeeper.properties'; exit 1; }

    sudo cp -f '$KAFKA_CFG_REPO_DIR/server.properties' '$KAFKA_HOME/config/server.properties'
    sudo cp -f '$KAFKA_CFG_REPO_DIR/zookeeper.properties' '$KAFKA_HOME/config/zookeeper.properties'
    sudo chown root:root '$KAFKA_HOME/config/server.properties' '$KAFKA_HOME/config/zookeeper.properties' || true
  "

  echo "[0.8/5] Push Hadoop configs to worker1"
  for f in core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml; do
    echo "  -> push $f to worker1"
    ssh -o BatchMode=yes -o StrictHostKeyChecking=no "$WORKER1_SSH" \
      "sudo tee '$HADOOP_CONF_DIR/$f' >/dev/null" < "$HADOOP_CFG_REPO_DIR/$f"
  done

  echo "[0.9/5] Ensure /mnt mounted + base dirs exist (master + worker1)"
  run_master "
    set -euo pipefail
    echo '--- master: mount check ---'
    mountpoint -q /mnt || { echo 'ERROR: /mnt is not a mountpoint on master.'; mount | grep ' on /mnt ' || true; exit 1; }

    sudo -n mkdir -p /mnt/hdfs/namenode /mnt/yarn/local /mnt/yarn/logs
    sudo -n chown -R adm-mcsc:adm-mcsc /mnt/hdfs /mnt/yarn
    sudo -n chmod 755 /mnt/hdfs /mnt/yarn /mnt/hdfs/namenode /mnt/yarn/local /mnt/yarn/logs

    for d in /mnt/hdfs /mnt/yarn /mnt/hdfs/namenode /mnt/yarn/local /mnt/yarn/logs; do
      test -w \"\$d\" || { echo \"ERROR: \$d is not writable on master.\"; ls -ld /mnt /mnt/hdfs /mnt/yarn \"\$d\"; exit 1; }
    done

    echo 'OK: master /mnt subdirs writable'
    ls -ld /mnt /mnt/hdfs /mnt/yarn /mnt/hdfs/namenode /mnt/yarn/local /mnt/yarn/logs || true
  "

  run_worker1 <<'EOF'
set -euo pipefail
echo '--- worker1: mount check ---'
mountpoint -q /mnt || { echo 'ERROR: /mnt is not a mountpoint on worker1.'; mount | grep ' on /mnt ' || true; exit 1; }

sudo -n mkdir -p /mnt/hdfs/datanode /mnt/yarn/local /mnt/yarn/logs
sudo -n chown -R adm-mcsc:adm-mcsc /mnt/hdfs /mnt/yarn
sudo -n chmod 755 /mnt/hdfs /mnt/yarn /mnt/hdfs/datanode /mnt/yarn/local /mnt/yarn/logs

for d in /mnt/hdfs /mnt/yarn /mnt/hdfs/datanode /mnt/yarn/local /mnt/yarn/logs; do
  test -w "$d" || { echo "ERROR: $d is not writable on worker1."; ls -ld /mnt /mnt/hdfs /mnt/yarn "$d"; exit 1; }
done

echo 'OK: worker1 /mnt subdirs writable'
ls -ld /mnt /mnt/hdfs /mnt/yarn /mnt/hdfs/datanode /mnt/yarn/local /mnt/yarn/logs || true
EOF

  echo "[1/5] Start HDFS (NameNode on master, DataNode on worker1)"
  run_master "
    set -euo pipefail
    export HADOOP_HOME='$HADOOP_HOME'
    export HADOOP_CONF_DIR='$HADOOP_CONF_DIR'
    export PATH=\"\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH\"

    nn_name=\$(hdfs getconf -confKey dfs.namenode.name.dir 2>/dev/null || true)
    nn_edits=\$(hdfs getconf -confKey dfs.namenode.edits.dir 2>/dev/null || true)

    echo \"dfs.namenode.name.dir=\$nn_name\"
    echo \"dfs.namenode.edits.dir=\$nn_edits\"

    [[ -n \"\$nn_edits\" ]] || nn_edits=\"\$nn_name\"

    for p in \${nn_name//,/ } \${nn_edits//,/ }; do
      p=\"\${p#file://}\"
      p=\"\${p#file:}\"
      [[ -n \"\$p\" ]] || continue
      sudo -n mkdir -p \"\$p\"
      sudo -n chown -R adm-mcsc:adm-mcsc \"\$p\"
      sudo -n chmod 755 \"\$p\"
    done

    hdfs --daemon start namenode
    sleep 2

    if ! jps | grep -q NameNode; then
      echo 'ERROR: NameNode not running'
      log=\$(ls -1t \"\$HADOOP_HOME/logs\"/hadoop-*-namenode-*.log 2>/dev/null | head -n 1 || true)
      echo \"NN_LOG=\$log\"
      [[ -n \"\$log\" ]] && tail -n 160 \"\$log\" || true
      exit 1
    fi

    # 给 yarn/spark 常用目录
    hdfs dfs -mkdir -p /tmp 2>/dev/null || true
    hdfs dfs -chmod 1777 /tmp 2>/dev/null || true
  "

  run_worker1 <<'EOF'
set -euo pipefail
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

# --- read dfs.datanode.data.dir from hdfs-site.xml (fallback to /mnt/hdfs/datanode) ---
dn_dirs="$(awk '
  BEGIN{inprop=0; nameok=0}
  /<property>/{inprop=1; nameok=0}
  inprop && /<name>dfs.datanode.data.dir<\/name>/{nameok=1}
  nameok && /<value>/{gsub(/.*<value>/,""); gsub(/<\/value>.*/,""); print; exit}
  /<\/property>/{inprop=0; nameok=0}
' "$HADOOP_CONF_DIR/hdfs-site.xml" | tr -d '[:space:]')"

[[ -n "$dn_dirs" ]] || dn_dirs="file:///mnt/hdfs/datanode"

IFS=',' read -r -a dn_arr <<< "$dn_dirs"

echo "dfs.datanode.data.dir=$dn_dirs"
for d in "${dn_arr[@]}"; do
  d="${d#file://}"
  d="${d#file:}"
  [[ -n "$d" ]] || continue
  sudo -n mkdir -p "$d"
  sudo -n chown -R adm-mcsc:adm-mcsc "$d"
  sudo -n chmod 755 "$d"
done

echo "=== datanode status(before) ==="
hdfs --daemon status datanode && echo "DataNode already running" || echo "DataNode not running"

if ! hdfs --daemon status datanode >/dev/null 2>&1; then
  hdfs --daemon start datanode || true
fi

sleep 3
jps | egrep "DataNode|Jps" || true
sudo -n ss -lntp | egrep ":(9866|9864|9867)\b" || ss -lntp | egrep ":(9866|9864|9867)\b" || true

if ! jps | grep -q DataNode; then
  echo "ERROR: DataNode not running after start"
  log=$(ls -1t "$HADOOP_HOME"/logs/hadoop-*-datanode-*.log 2>/dev/null | head -n 1 || true)
  echo "DN_LOG=$log"
  [[ -n "$log" ]] && tail -n 220 "$log" || true
  exit 1
fi
EOF

  echo "[1.5/5] Wait HDFS leave safemode"
  run_master "
    export HADOOP_HOME='$HADOOP_HOME'
    export HADOOP_CONF_DIR='$HADOOP_CONF_DIR'
    export PATH=\"\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH\"

    for i in {1..60}; do
      sm=\$(hdfs dfsadmin -safemode get 2>/dev/null || true)
      echo \"safemode: \$sm\"
      echo \"\$sm\" | grep -qi OFF && exit 0
      sleep 1
    done

    live=\$(hdfs dfsadmin -report 2>/dev/null | awk '/Live datanodes/ {print \$3}' | tr -d '()')
    if [[ \"\${live:-0}\" == \"0\" ]]; then
      echo 'ERROR: no live DataNode detected.'
      exit 1
    fi

    echo 'SafeMode still ON after 60s -> forcing leave (demo mode)'
    hdfs dfsadmin -safemode leave
  "

  echo "[2/5] Start YARN (RM on master, NM on worker1)"
  run_master "
    set -euo pipefail
    export HADOOP_HOME='$HADOOP_HOME'
    export HADOOP_CONF_DIR='$HADOOP_CONF_DIR'
    export PATH=\"\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH\"

    nm_local=\$(awk '
      BEGIN{inprop=0; nameok=0}
      /<property>/{inprop=1; nameok=0}
      inprop && /<name>yarn.nodemanager.local-dirs<\/name>/{nameok=1}
      nameok && /<value>/{gsub(/.*<value>/,\"\" ); gsub(/<\\/value>.*/,\"\" ); print; exit}
      /<\\/property>/{inprop=0; nameok=0}
    ' \"\$HADOOP_CONF_DIR/yarn-site.xml\" | tr -d \"\\n\\r\")

    nm_logs=\$(awk '
      BEGIN{inprop=0; nameok=0}
      /<property>/{inprop=1; nameok=0}
      inprop && /<name>yarn.nodemanager.log-dirs<\/name>/{nameok=1}
      nameok && /<value>/{gsub(/.*<value>/,\"\" ); gsub(/<\\/value>.*/,\"\" ); print; exit}
      /<\\/property>/{inprop=0; nameok=0}
    ' \"\$HADOOP_CONF_DIR/yarn-site.xml\" | tr -d \"\\n\\r\")

    [[ -n \"\$nm_local\" ]] || nm_local=\"/mnt/yarn/local\"
    [[ -n \"\$nm_logs\"  ]] || nm_logs=\"/mnt/yarn/logs\"

    echo \"yarn.nodemanager.local-dirs=\$nm_local\"
    echo \"yarn.nodemanager.log-dirs=\$nm_logs\"

    for p in \${nm_local//,/ } \${nm_logs//,/ }; do
      [[ -n \"\$p\" ]] || continue
      sudo -n mkdir -p \"\$p\"
      sudo -n chown -R adm-mcsc:adm-mcsc \"\$p\"
      sudo -n chmod 755 \"\$p\"
    done

    yarn --daemon start resourcemanager
    yarn --daemon status resourcemanager || true
  "

  run_worker1 <<'EOF'
set -euo pipefail
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

nm_local="$(awk '
  BEGIN{inprop=0; nameok=0}
  /<property>/{inprop=1; nameok=0}
  inprop && /<name>yarn.nodemanager.local-dirs<\/name>/{nameok=1}
  nameok && /<value>/{gsub(/.*<value>/,""); gsub(/<\/value>.*/,""); print; exit}
  /<\/property>/{inprop=0; nameok=0}
' "$HADOOP_CONF_DIR/yarn-site.xml" | tr -d '\n\r')"

nm_logs="$(awk '
  BEGIN{inprop=0; nameok=0}
  /<property>/{inprop=1; nameok=0}
  inprop && /<name>yarn.nodemanager.log-dirs<\/name>/{nameok=1}
  nameok && /<value>/{gsub(/.*<value>/,""); gsub(/<\/value>.*/,""); print; exit}
  /<\/property>/{inprop=0; nameok=0}
' "$HADOOP_CONF_DIR/yarn-site.xml" | tr -d '\n\r')"

# fallback 到 /mnt
[[ -n "$nm_local" ]] || nm_local="/mnt/yarn/local"
[[ -n "$nm_logs"  ]] || nm_logs="/mnt/yarn/logs"

echo "yarn.nodemanager.local-dirs=$nm_local"
echo "yarn.nodemanager.log-dirs=$nm_logs"

for p in ${nm_local//,/ } ${nm_logs//,/ }; do
  [[ -n "$p" ]] || continue
  sudo -n mkdir -p "$p"
  sudo -n chown -R adm-mcsc:adm-mcsc "$p"
  sudo -n chmod 755 "$p"
done

echo "=== nodemanager status(before) ==="
yarn --daemon status nodemanager && echo "NodeManager already running" || echo "NodeManager not running"

if ! yarn --daemon status nodemanager >/dev/null 2>&1; then
  yarn --daemon start nodemanager || true
fi

sleep 3
jps | egrep "NodeManager|Jps" || true
sudo -n ss -lntp | egrep ":(8042)\b" || ss -lntp | egrep ":(8042)\b" || true

if ! jps | grep -q NodeManager; then
  echo "ERROR: NodeManager not running after start"
  log=$(ls -1t "$HADOOP_HOME"/logs/*nodemanager*log 2>/dev/null | head -n 1 || true)
  echo "NM_LOG=$log"
  [[ -n "$log" ]] && tail -n 220 "$log" || true
  exit 1
fi
EOF

  echo "[3/5] Start Kafka (ZK + broker) on master"
  run_master "
    sudo mkdir -p /data/kafka/broker /data/zookeeper /var/log/kafka
    sudo chown -R adm-mcsc:adm-mcsc /data/kafka/broker /data/zookeeper /var/log/kafka

    mkdir -p ~/pipeline_logs
    cd ~/pipeline_logs

    export KAFKA_HOME='$KAFKA_HOME'
    export LOG_DIR=/var/log/kafka

    if ss -lntp | grep -q ':2181'; then
      echo 'ZK already running'
    else
      \$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon \$KAFKA_HOME/config/zookeeper.properties
      sleep 2
    fi

    \$KAFKA_HOME/bin/kafka-server-start.sh -daemon \$KAFKA_HOME/config/server.properties

    for i in {1..30}; do
      if ss -lntp | grep -q ':9092'; then
        echo 'Kafka is listening on 9092'
        break
      fi
      sleep 1
    done

    ss -ltnp | egrep ':2181|:9092' || true

    if ! ss -ltnp | grep -q ':9092'; then
      echo 'ERROR: Kafka broker not listening on 9092'
      echo '== kafkaServer.out (daemon stdout) =='
      tail -n 200 ~/pipeline_logs/kafkaServer.out 2>/dev/null || true
      echo '== /var/log/kafka (log4j) =='
      ls -lt /var/log/kafka | head -n 30 || true
      tail -n 200 /var/log/kafka/server.log 2>/dev/null || true
      exit 1
    fi
  "

  echo "[3.5/5] Wait for Kafka broker ready on $KAFKA_BOOTSTRAP"
  for i in {1..30}; do
    if timeout 2 kafka-broker-api-versions.sh --bootstrap-server "$KAFKA_BOOTSTRAP" >/dev/null 2>&1; then
      echo "Kafka is ready"
      break
    fi
    sleep 1
  done

  echo "[4/5] Ensure topic exists"
  run_master "$(cat <<'CMD'
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --list | grep -qx "$KAFKA_TOPIC" || \
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --create --topic "$KAFKA_TOPIC" --partitions 1 --replication-factor 1
CMD
  )"

  echo "[5/5] Start Producer + Submit Spark (YARN $SPARK_DEPLOY_MODE) + Sanity"
  run_master "$(cat <<'CMD'
set -euo pipefail
source "$HOME/BigData/scripts/_load_env.sh" 2>/dev/null || true
mkdir -p ~/pipeline_logs ~/pipeline_pids

: "${PRODUCER_ABS:?PRODUCER_ABS not set}"

if [[ -f ~/pipeline_pids/producer.pid ]] && ps -p "$(cat ~/pipeline_pids/producer.pid)" >/dev/null 2>&1; then
  echo "Producer already running (pid=$(cat ~/pipeline_pids/producer.pid))"
else
  chmod +x "${PRODUCER_ABS}" 2>/dev/null || true

  if [[ -n "${PRODUCER_PY:-}" ]]; then
    nohup "${PRODUCER_PY}" -u "${PRODUCER_ABS}" > ~/pipeline_logs/producer.log 2>&1 &
  else
    nohup env PYTHONUNBUFFERED=1 "${PRODUCER_ABS}" > ~/pipeline_logs/producer.log 2>&1 &
  fi

  echo $! > ~/pipeline_pids/producer.pid
  sleep 1

  pid="$(cat ~/pipeline_pids/producer.pid)"
  if ! ps -p "$pid" >/dev/null 2>&1; then
    echo "ERROR: Producer failed to start"
    tail -n 200 ~/pipeline_logs/producer.log || true
    exit 1
  fi
  echo "producer pid=$pid"
fi

export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP}"
export KAFKA_TOPIC="${KAFKA_TOPIC}"
export HDFS_OUT_DIR="${HDFS_OUT_DIR}"
export HDFS_CKPT_DIR="${HDFS_CKPT_DIR}"

spark-submit --master yarn --deploy-mode "${SPARK_DEPLOY_MODE}" \
  --name "${SPARK_APP_NAME}" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
  ${SPARK_EXTRA_CONF:-} \
  "${SPARK_JOB_ABS}"

echo "[5.5/5] Sanity check Kafka has messages + Wait Spark app become RUNNING"

timeout 10 kafka-console-consumer.sh --bootstrap-server "${KAFKA_BOOTSTRAP}" \
  --topic "${KAFKA_TOPIC}" --max-messages 1 --timeout-ms 8000 >/dev/null 2>&1 \
  || echo "WARN: no message consumed in 10s (producer may be idle or topic empty)"

appid="$(yarn application -list 2>/dev/null | awk -v n="${SPARK_APP_NAME}" '$0 ~ n {print $1}' | tail -n 1 || true)"
echo "spark appid=${appid:-<none>}"

for i in {1..30}; do
  [[ -z "${appid:-}" ]] && { sleep 1; continue; }
  st="$(yarn application -status "$appid" 2>/dev/null | awk -F': ' '/State/ {print $2}' | tr -d '\r')"
  echo "state=$st"
  [[ "$st" == "RUNNING" ]] && break
  [[ "$st" == "FAILED" || "$st" == "KILLED" ]] && { yarn application -status "$appid"; exit 1; }
  sleep 1
done
CMD
  )"

  echo "[OK] Started."
  echo "UI: run ./scripts/pipeline.sh tunnel then open http://localhost:8088"
}


stop_all() {
  echo "[1/4] Kill Spark apps on YARN (match by name)"
  run_master "
    set -euo pipefail
    ids=\$(yarn application -list 2>/dev/null | awk -v n='$SPARK_APP_NAME' '\$0 ~ n {print \$1}')
    for id in \$ids; do
      echo \"killing yarn app \$id\"
      yarn application -kill \"\$id\" || true
    done
  "

  echo "[2/4] Stop Producer"
  run_master "
    set -euo pipefail
    if [[ -f ~/pipeline_pids/producer.pid ]]; then
      pid=\$(cat ~/pipeline_pids/producer.pid 2>/dev/null || true)
      if [[ -n \"\$pid\" ]] && ps -p \"\$pid\" >/dev/null 2>&1; then
        echo \"stopping producer pid=\$pid\"
        kill \"\$pid\" 2>/dev/null || true
        for i in {1..5}; do
          ps -p \"\$pid\" >/dev/null 2>&1 || break
          sleep 1
        done
        if ps -p \"\$pid\" >/dev/null 2>&1; then
          echo \"producer still alive -> kill -9\"
          kill -9 \"\$pid\" 2>/dev/null || true
        fi
      fi
      rm -f ~/pipeline_pids/producer.pid
    fi
  "

  echo "[3/4] Stop Kafka + ZK"
  run_master "
    set -euo pipefail
    cd $KAFKA_HOME
    bin/kafka-server-stop.sh || true
    sleep 2
    pkill -f 'kafka\\.Kafka' || true
    sleep 1
    pkill -9 -f 'kafka\\.Kafka' || true

    bin/zookeeper-server-stop.sh || true
    sleep 2
    pkill -f 'org\\.apache\\.zookeeper' || true
    sleep 1
    pkill -9 -f 'org\\.apache\\.zookeeper' || true
  "

  echo "[4/4] Stop YARN + HDFS"
  run_worker1 "yarn --daemon stop nodemanager || true"
  run_worker1 "hdfs --daemon stop datanode || true"

  run_master "
    set -euo pipefail
    yarn --daemon stop nodemanager || true
    yarn --daemon stop resourcemanager || true

    hdfs --daemon stop secondarynamenode || true
    hdfs --daemon stop namenode || true
  "

  echo "[OK] Stopped."
}


status() {
  local fail=0

  echo "=== Local daemons (jps) ==="
  jps || true
  echo

  echo "=== YARN NodeManagers ==="
  if jps | grep -q ResourceManager; then
    timeout 5s yarn node -list -all || { echo "WARN: yarn node -list failed"; fail=1; }
  else
    echo "OK: ResourceManager not running (stopped)"
  fi
  echo

  echo "=== YARN Applications (Spark) ==="
  if jps | grep -q ResourceManager; then
    timeout 5s yarn application -list | grep -i "$SPARK_APP_NAME" || true
  else
    echo "OK: YARN not running, no apps expected"
  fi
  echo

  echo "=== Kafka topic describe ==="
  if ss -lntp 2>/dev/null | grep -q ':9092'; then
    timeout 5s kafka-topics.sh --describe --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$KAFKA_TOPIC" \
      || { echo "WARN: kafka-topics describe failed"; fail=1; }
  else
    echo "OK: Kafka not listening on 9092 (stopped)"
  fi
  echo

  echo "=== HDFS output size ==="
  if jps | grep -q NameNode; then
    timeout 5s hdfs dfs -du -h "$(echo "$HDFS_OUT_DIR" | sed 's#hdfs:///##')" 2>/dev/null || { echo "WARN: hdfs du failed"; fail=1; }
  else
    echo "OK: NameNode not running (stopped)"
  fi
  echo

  echo "=== Producer last logs ==="
  if [[ -f ~/pipeline_pids/producer.pid ]] && ps -p "$(cat ~/pipeline_pids/producer.pid)" >/dev/null 2>&1; then
    echo "OK: Producer still running (pid=$(cat ~/pipeline_pids/producer.pid))"
    fail=1
  else
    echo "WARN: Producer not running"
  fi
  tail -n 20 ~/pipeline_logs/producer.log 2>/dev/null || true
  echo

  return $fail
}


case "${1:-}" in
  start) start_all ;;
  stop) stop_all ;;
  status) status ;;
  tunnel) tunnel ;;
  *) usage; exit 1 ;;
esac
