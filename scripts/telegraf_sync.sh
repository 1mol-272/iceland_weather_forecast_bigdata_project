#!/usr/bin/env bash
set -euo pipefail
BIGDATA_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$BIGDATA_ROOT/scripts/_load_env.sh"

SRC="$BIGDATA_ROOT/config/telegraf/weather_kafka.conf"
DST_DIR="/etc/telegraf/telegraf.d"
DST="$DST_DIR/weather_kafka.conf"

sudo mkdir -p "$DST_DIR"

envsubst < "$SRC" | sudo tee "$DST" >/dev/null

sudo systemctl restart telegraf
sudo systemctl status telegraf --no-pager
