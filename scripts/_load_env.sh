#!/usr/bin/env bash
set -euo pipefail

BIGDATA_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Project defaults (commit)
set -a
source "$BIGDATA_ROOT/config/pipeline.env"
set +a

# Machine-specific overrides (do not commit)
if [[ -f "$BIGDATA_ROOT/env/local.env" ]]; then
  set -a
  source "$BIGDATA_ROOT/env/local.env"
  set +a
else
  echo "Missing env/local.env. Copy env/local.env.example -> env/local.env and edit it." >&2
  exit 1
fi

# Repo paths 
PRODUCER_ABS="$BIGDATA_ROOT/${PRODUCER_REL#/}"
SPARK_JOB_ABS="$BIGDATA_ROOT/${SPARK_JOB_REL#/}"
HADOOP_CFG_REPO_DIR="$BIGDATA_ROOT/${HADOOP_CFG_REL#/}"
KAFKA_CFG_REPO_DIR="$BIGDATA_ROOT/${KAFKA_REL#/}"
SPARK_KPI_JOB_ABS="$BIGDATA_ROOT/${SPARK_KPI_JOB_REL#/}"

# local PATH on master
export PATH="$KAFKA_HOME/bin:$SPARK_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

run_master() {
  bash -lc "$*"
}

run_worker1() {
  local script b64

  if [[ $# -gt 0 ]]; then
    script="$*"
  else
    if [[ -t 0 ]]; then
      echo "run_worker1: need heredoc or command string" >&2
      return 2
    fi
    script="$(cat)"
  fi

  b64="$(printf '%s' "$script" | base64 -w0)"

  /usr/bin/ssh -tt \
    -o RequestTTY=force \
    -o BatchMode=yes \
    -o StrictHostKeyChecking=no \
    -o LogLevel=ERROR \
    "$WORKER1_SSH" \
    "SCRIPT_B64='$b64' bash -s" <<'REMOTE'
set -euo pipefail

stty -echo 2>/dev/null || true

printf '%s' "$SCRIPT_B64" | base64 -d | bash -se

rc=$?

stty echo 2>/dev/null || true
exit $rc
REMOTE
}





