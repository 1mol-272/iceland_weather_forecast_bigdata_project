cat > ~/check_hadoop.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

CONF_DIR="${HADOOP_HOME:-/usr/local/hadoop}/etc/hadoop"
echo "== Using CONF_DIR: $CONF_DIR =="

ok(){ echo "[OK] $*"; }
fail(){ echo "[FAIL] $*"; FAILED=1; }
FAILED=0

echo "== 1) fs.defaultFS 应为 hdfs://master:9000 =="
val=$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || true)
[[ "$val" == "hdfs://master:9000" ]] && ok "fs.defaultFS=$val" || fail "fs.defaultFS=$val（应为 hdfs://master:9000）"

echo "== 2) HDFS 关键参数 =="
rep=$(hdfs getconf -confKey dfs.replication 2>/dev/null || true)
[[ "$rep" == "2" ]] && ok "dfs.replication=$rep" || fail "dfs.replication=$rep(应为 2）"
nn=$(hdfs getconf -confKey dfs.namenode.name.dir 2>/dev/null || true)
[[ "$nn" == "file:///usr/local/hadoop/data/namenode" ]] && ok "dfs.namenode.name.dir=$nn" || fail "namenode.dir=$nn（应为 file:///usr/local/hadoop/data/namenode）"
dn=$(hdfs getconf -confKey dfs.datanode.data.dir 2>/dev/null || true)
[[ "$dn" == "file:///usr/local/hadoop/data/datanode" ]] && ok "dfs.datanode.data.dir=$dn" || fail "datanode.dir=$dn（应为 file:///usr/local/hadoop/data/datanode）"

echo "== 3) YARN / MapReduce 关键参数(grep 方式校验） =="
grepq(){ grep -Eqs "$2" "$1" && ok "$1: 匹配 $2" || fail "$1: 未匹配 $2"; }
grepq "$CONF_DIR/mapred-site.xml" '<name>mapreduce.framework.name</name>.*<value>yarn</value>'
grepq "$CONF_DIR/mapred-site.xml" '<name>yarn.app.mapreduce.am.env</name>.*HADOOP_MAPRED_HOME=/usr/local/hadoop'
grepq "$CONF_DIR/mapred-site.xml" '<name>mapreduce.map.env</name>.*HADOOP_MAPRED_HOME=/usr/local/hadoop'
grepq "$CONF_DIR/mapred-site.xml" '<name>mapreduce.reduce.env</name>.*HADOOP_MAPRED_HOME=/usr/local/hadoop'
grepq "$CONF_DIR/yarn-site.xml" '<name>yarn.nodemanager.aux-services</name>.*<value>mapreduce_shuffle</value>'
grepq "$CONF_DIR/yarn-site.xml" '<name>yarn.resourcemanager.hostname</name>.*<value>master</value>'
grepq "$CONF_DIR/yarn-site.xml" '<name>yarn.resourcemanager.webapp.address</name>.*<value>master:8088</value>'
grepq "$CONF_DIR/yarn-site.xml" '<name>yarn.log-aggregation-enable</name>.*<value>true</value>'

echo "== 4) 目录存在性与权限（示例检查） =="
for d in /usr/local/hadoop/data/namenode /usr/local/hadoop/data/datanode; do
  [[ -d "$d" ]] && ok "存在 $d" || fail "缺失目录 $d"
done

echo "== 5) workers 列表 与 解析/SSH连通性 =="
workers_file="$CONF_DIR/workers"
if [[ -f "$workers_file" ]]; then
  echo "workers 文件内容："; sed '/^\s*$/d;/^\s*#/d' "$workers_file"
  WCOUNT=$(sed '/^\s*$/d;/^\s*#/d' "$workers_file" | wc -l)
  [[ $WCOUNT -ge 1 ]] && ok "workers 至少 1 台：$WCOUNT" || fail "workers 为空"
  [[ "$rep" =~ ^[0-9]+$ && "$rep" -le "$WCOUNT" ]] && ok "dfs.replication=$rep <= workers=$WCOUNT" || fail "dfs.replication=$rep > workers=$WCOUNT"
  while read -r h; do
    [[ -z "$h" || "$h" =~ ^# ]] && continue
    getent hosts "$h" >/dev/null 2>&1 && ok "$h 可解析" || fail "$h 解析失败（/etc/hosts）"
    ssh -o BatchMode=yes -o ConnectTimeout=3 "$h" "echo SSH_OK" >/dev/null 2>&1 && ok "$h 免密SSH OK" || fail "$h 免密SSH 失败（ssh-copy-id $h）"
  done < <(sed '/^\s*$/d;/^\s*#/d' "$workers_file")
else
  fail "缺少 $workers_file"
fi

echo "== 6) master 主机名与 /etc/hosts =="
hname=$(hostname)
[[ "$hname" == "master" ]] && ok "hostname=master" || fail "hostname=$hname"
getent hosts master >/dev/null 2>&1 && ok "master 可解析" || fail "master 解析失败（/etc/hosts）"

echo "== 检查完成 =="
exit $FAILED
EOF