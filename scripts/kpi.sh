#!/usr/bin/env bash
set -euo pipefail

BIGDATA_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$BIGDATA_ROOT/scripts/_load_env.sh"

cmd="${1:-}"
if [[ -z "$cmd" ]]; then
  echo "Usage: $0 {start|stop|status|logs}"
  exit 1
fi

start_job() {
  echo "[INFO] Submitting KPI Spark job to YARN..."
  echo "       job: $SPARK_KPI_JOB_ABS"
  echo "       app: ${SPARK_KPI_APP_NAME}"

  run_master "$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode ${SPARK_DEPLOY_MODE:-client} \
    --name ${SPARK_KPI_APP_NAME:-WeatherKPIToInflux} \
    ${SPARK_SUBMIT_CONF:-} \
    --conf \"spark.yarn.appMasterEnv.KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP}\" \
    --conf \"spark.executorEnv.KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP}\" \
    --conf \"spark.yarn.appMasterEnv.KAFKA_TOPIC=${KAFKA_TOPIC}\" \
    --conf \"spark.executorEnv.KAFKA_TOPIC=${KAFKA_TOPIC}\" \
    --conf \"spark.yarn.appMasterEnv.INFLUX_URL=${INFLUX_URL}\" \
    --conf \"spark.executorEnv.INFLUX_URL=${INFLUX_URL}\" \
    --conf \"spark.yarn.appMasterEnv.INFLUX_ORG=${INFLUX_ORG}\" \
    --conf \"spark.executorEnv.INFLUX_ORG=${INFLUX_ORG}\" \
    --conf \"spark.yarn.appMasterEnv.INFLUX_BUCKET=${INFLUX_BUCKET}\" \
    --conf \"spark.executorEnv.INFLUX_BUCKET=${INFLUX_BUCKET}\" \
    --conf \"spark.yarn.appMasterEnv.EXTREME_GUST_THRESHOLD=${EXTREME_GUST_THRESHOLD}\" \
    --conf \"spark.executorEnv.EXTREME_GUST_THRESHOLD=${EXTREME_GUST_THRESHOLD}\" \
    --conf \"spark.yarn.appMasterEnv.SPARK_KPI_APP_NAME=${SPARK_KPI_APP_NAME}\" \
    --conf \"spark.executorEnv.SPARK_KPI_APP_NAME=${SPARK_KPI_APP_NAME}\" \
    --conf \"spark.yarn.appMasterEnv.INFLUX_TOKEN=${INFLUX_TOKEN}\" \
    --conf \"spark.executorEnv.INFLUX_TOKEN=${INFLUX_TOKEN}\" \
    \"$SPARK_KPI_JOB_ABS\""
  echo "[OK] Submitted. Use: $0 status"
}

stop_job() {
  echo "[INFO] Killing KPI app(s) by name: ${SPARK_KPI_APP_NAME}"
  run_master "yarn application -list 2>/dev/null | awk 'NR>2{print \$1\" \"\$2\" \"\$3\" \"\$4}' | grep -F '${SPARK_KPI_APP_NAME}' || true"
  run_master "yarn application -list 2>/dev/null | awk 'NR>2{print \$1\" \"\$2\" \"\$3\" \"\$4}' | grep -F '${SPARK_KPI_APP_NAME}' | awk '{print \$1}' | xargs -r -n1 yarn application -kill"
}

status_job() {
  echo "=== KPI app status (YARN) ==="
  run_master "yarn application -list 2>/dev/null | head -n 50"
  echo
  run_master "yarn application -list 2>/dev/null | grep -F '${SPARK_KPI_APP_NAME}' || echo '[INFO] No KPI app found.'"
}

logs_job() {
  echo "[INFO] Showing last KPI app id..."
  run_master "APP=\$(yarn application -list 2>/dev/null | grep -F '${SPARK_KPI_APP_NAME}' | awk '{print \$1}' | head -n1); \
    if [[ -z \"\$APP\" ]]; then echo 'No running KPI app'; exit 0; fi; \
    echo \"APP=\$APP\"; \
    yarn logs -applicationId \"\$APP\" 2>/dev/null | tail -n 120"
}

case "$cmd" in
  start)  start_job ;;
  stop)   stop_job ;;
  status) status_job ;;
  logs)   logs_job ;;
  *) echo "Usage: $0 {start|stop|status|logs}"; exit 1 ;;
esac
