#!/usr/bin/env bash
set -euo pipefail

BIGDATA_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$BIGDATA_ROOT/scripts/_load_env.sh"

cmd="${1:-}"
if [[ -z "$cmd" ]]; then
  echo "Usage: $0 {start|stop|status|logs}"
  exit 1
fi

start_all() {
  echo "[1/3] Start InfluxDB..."
  run_master "sudo systemctl start influxdb; sudo systemctl status influxdb --no-pager || true; ss -lntp | grep 8086 || true"

  echo
  echo "[2/3] Restart Telegraf..."
  run_master "sudo systemctl daemon-reload; sudo systemctl restart telegraf; sudo systemctl status telegraf --no-pager || true"

  echo
  echo "[3/3] Start Grafana..."
  run_master "sudo systemctl start grafana-server; sudo systemctl status grafana-server --no-pager || true; ss -lntp | grep 3000 || true"
}

stop_all() {
  echo "[INFO] Stop Grafana..."
  run_master "sudo systemctl stop grafana-server || true"
  echo "[INFO] Stop Telegraf..."
  run_master "sudo systemctl stop telegraf || true"
  echo "[INFO] Stop InfluxDB..."
  run_master "sudo systemctl stop influxdb || true"
}

status_all() {
  run_master "echo '=== Ports ==='; ss -lntp | egrep '(:8086|:3000)' || true"
  echo
  run_master "echo '=== Services ==='; for s in influxdb telegraf grafana-server; do echo \"--- \$s ---\"; systemctl is-enabled \$s 2>/dev/null || true; systemctl is-active \$s 2>/dev/null || true; done"
}

logs_telegraf() {
  run_master "sudo journalctl -u telegraf -n 80 --no-pager -l"
}

case "$cmd" in
  start)  start_all ;;
  stop)   stop_all ;;
  status) status_all ;;
  logs)   logs_telegraf ;;
  *) echo "Usage: $0 {start|stop|status|logs}"; exit 1 ;;
esac
