#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_load_env.sh"

echo "=== HEALTHCHECK @ $(date -u '+%F %T UTC') ==="
echo

echo "[MASTER] hostname/jps:"
hostname
jps || true
echo

echo "[WORKER1] hostname/jps:"
if ! run_worker1 <<'EOF'
set -euo pipefail
hostname
jps || true
EOF
then
  echo "WARN: worker1 unreachable or command failed."
  echo "      Try manually:"
  echo "      ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no \"$WORKER1_SSH\" \"hostname; jps || true\""
fi
echo

echo "[Ports on MASTER] 9092(Kafka) 8086(Influx) 3000(Grafana):"
ss -lntp | egrep ':(9092|8086|3000)\b' || true
echo

echo "[Systemd] influxdb / telegraf / grafana-server:"
for svc in influxdb telegraf grafana-server; do
  echo "--- $svc ---"
  systemctl is-active "$svc" 2>/dev/null || true
done
echo

echo "[Telegraf recent errors]:"
if journalctl -u telegraf -n 80 --no-pager -l 2>/dev/null | egrep -i ' E! |error|unauthorized|failed to write' >/dev/null; then
  journalctl -u telegraf -n 80 --no-pager -l | egrep -i ' E! |error|unauthorized|failed to write' || true
else
  echo "(no recent errors)"
fi
echo

echo "[YARN] running apps (top 20):"
if jps | grep -q ResourceManager; then
  timeout 6s yarn application -list 2>/dev/null | head -n 25 || true
else
  echo "OK: ResourceManager not running"
fi
echo

echo "[HDFS] raw dir usage:"
if jps | grep -q NameNode; then
  timeout 6s hdfs dfs -du -h /data/weather/raw 2>/dev/null | head -n 30 || true
else
  echo "OK: NameNode not running"
fi
echo

