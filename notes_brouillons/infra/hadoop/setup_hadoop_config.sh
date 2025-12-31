#!/usr/bin/env bash
# 在 master 上、以 hadoop 用户执行：配置环境变量 + 初始化 Hadoop 配置并同步到 worker1

set -euo pipefail

### 1. 写入 hadoop 用户的环境变量到 ~/.bashrc
BASHRC="$HOME/.bashrc"

if ! grep -q "HADOOP_HOME=/usr/local/hadoop" "$BASHRC"; then
  echo "== 写入 Hadoop 相关环境变量到 ~/.bashrc =="

  cat >> "$BASHRC" << 'EOF'

# >>> Hadoop environment >>>
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
# <<< Hadoop environment <<<
EOF
else
  echo "== ~/.bashrc 中已包含 HADOOP_HOME, 跳过环境变量追加 =="
fi

# 让当前 shell 立刻生效这些变量
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

### 2. 创建 HDFS 数据 & 日志目录（master 本机）
echo "== 在 master 上创建 /usr/local/hadoop/{data,logs} 目录并设置权限 =="

sudo mkdir -p /usr/local/hadoop/data/namenode
sudo mkdir -p /usr/local/hadoop/data/datanode
sudo mkdir -p /usr/local/hadoop/logs

sudo chown -R hadoop:hadoop /usr/local/hadoop/data
sudo chown hadoop:hadoop /usr/local/hadoop/logs

sudo chmod -R 750 /usr/local/hadoop/data
sudo chmod 750 /usr/local/hadoop/logs

### 2b. 在 worker1 上创建 DataNode 数据 & 日志目录
echo "== 在 worker1 上创建 /usr/local/hadoop/{data,logs} 目录并设置权限 =="

ssh worker1 'sudo mkdir -p /usr/local/hadoop/data/datanode /usr/local/hadoop/logs'
ssh worker1 'sudo chown -R hadoop:hadoop /usr/local/hadoop/data'
ssh worker1 'sudo chown hadoop:hadoop /usr/local/hadoop/logs'
ssh worker1 'sudo chmod -R 750 /usr/local/hadoop/data'
ssh worker1 'sudo chmod 750 /usr/local/hadoop/logs'

### 3. 生成 Hadoop 配置文件
echo "== 写 hadoop-env.sh 中的 JAVA_HOME =="
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> "$HADOOP_CONF_DIR/hadoop-env.sh"

echo "== 生成 core-site.xml =="
cat > "$HADOOP_CONF_DIR/core-site.xml" << 'EOF'
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://master:9000</value>
  </property>
</configuration>
EOF

echo "== 生成 hdfs-site.xml =="
cat > "$HADOOP_CONF_DIR/hdfs-site.xml" << 'EOF'
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///usr/local/hadoop/data/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///usr/local/hadoop/data/datanode</value>
  </property>
</configuration>
EOF

echo "== 生成 mapred-site.xml =="
cp "$HADOOP_CONF_DIR/mapred-site.xml.template" "$HADOOP_CONF_DIR/mapred-site.xml"
cat > "$HADOOP_CONF_DIR/mapred-site.xml" << 'EOF'
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
EOF

echo "== 生成 yarn-site.xml =="
cat > "$HADOOP_CONF_DIR/yarn-site.xml" << 'EOF'
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>master</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
EOF

echo "== 写 workers 列表 =="
cat > "$HADOOP_CONF_DIR/workers" << 'EOF'
master
worker1
EOF

### 3. 同步配置 + bashrc 到 worker1
echo "== 同步配置和 ~/.bashrc 到 worker1 =="

cd ~
rsync -av "$HADOOP_HOME/etc/hadoop/" "hadoop@worker1:$HADOOP_HOME/etc/hadoop/"
rsync -av "$BASHRC" "hadoop@worker1:/home/hadoop/.bashrc"

echo "All done. 请重新登录 hadoop 用户, 或执行: source ~/.bashrc"
