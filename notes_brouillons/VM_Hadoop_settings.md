# 虚拟机配置 + hadoop （namenode + datanode）总体框架 + 配置 （免密 + 环境配置 + 创建目录）

## 一：登录信息

ESILV-MCSCIN5A1825-0030\
esilv-mcscin5a1825-0030.westeurope.cloudapp.azure.com;
10.0.0.36\
tengzhe.zhang@edu.devinci.fr

ESILV-MCSCIN5A1825-0031\
esilv-mcscin5a1825-0031.westeurope.cloudapp.azure.com;
10.0.0.37\
tengzhe.zhang@edu.devinci.fr

```bash
# 链接虚拟机
ssh adm-mcsc@esilv-mcscin5a1825-0030.westeurope.cloudapp.azure.com\
ssh adm-mcsc@esilv-mcscin5a1825-0031.westeurope.cloudapp.azure.com
```
ssh -L 8888:localhost:8888 adm-mcsc@esilv-mcscin5a1825-0030.westeurope.cloudapp.azure.com
---
## 二：总体框架
**VM1（master）：** Namenode + Datanode

**VM2（worker1）：** DataNode 

---
## 三：配置 （hadoop）

1. 设置名字
```bash
# 在 master 上
sudo hostnamectl set-hostname master
# 在 worker1 上
sudo hostnamectl set-hostname worker1

#在两台机器的 /etc/hosts 里加入彼此的私网 IP映射：
sudo bash -c 'cat >> /etc/hosts <<EOF
10.0.0.4 master
10.0.0.5 worker1
EOF'
```

2. 新建 hadoop 用户
```bash
sudo adduser hadoop
sudo usermod -aG sudo hadoop
sudo -i -u hadoop  # 切到 hadoop 用户
```

3. 安装基础依赖）
```bash
sudo apt update
sudo apt install -y openjdk-11-jdk ssh rsync wget tar
##校验 Java
java -version
```

4. 下载并安装 Hadoop**
```bash
cd ~ 
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
mv hadoop-3.3.6 hadoop
```

5. 设置免密
```bash
# 1) 给adm-mcsc账号配免密：master → master（回环）+ master → worker1 拷贝本机的 SSH 公钥到远程用户
# 解决：在 master 自己执行一次，把自己的公钥也装到 master 上
ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519   
ssh-copy-id adm-mcsc@master
ssh-copy-id adm-mcsc@worker1
# 验证
ssh -o BatchMode=yes master  'echo OK master'
ssh -o BatchMode=yes worker1 'echo OK worker1'
--> 这能保证 start-dfs.sh/start-yarn.sh 通过 SSH 回连 master 时不再要密码。
stop-yarn.sh; stop-dfs.sh
start-dfs.sh; start-yarn.sh

# 2) 从本机登录 master 免密
# 解决：在本机上把“电脑的公钥”拷到外网主机（老师给的 FQDN）：
# 在本机 PowerShell
type "$env:USERPROFILE\.ssh\id_ed25519.pub" |
ssh adm-mcsc@esilv-mcscin5a1825-0031.westeurope.cloudapp.azure.com `
"mkdir -p ~/.ssh && chmod 700 ~/.ssh && cat >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"
# 验证
ssh -o BatchMode=yes adm-mcsc@esilv-mcscin5a1825-0030.westeurope.cloudapp.azure.com "echo OK"

# 3) 给hadoop账号配免密：master → master（回环）+ master → worker1 拷贝本机的 SSH 公钥到远程用户
ssh-copy-id hadoop@master
ssh-copy-id hadoop@worker1
# 验证
ssh master "hostname"
ssh worker1 "hostname"
```

6. 配置 hadoop 自己用的环境&文件 <- 在 master 上给 Hadoop 做配置后，同步到worker1
一点过要记得检查目录，用户名是不是一致 <- 因为不一致反复配置了好几次
```bash
sudo -i -u hadoop

cat >> ~/.bashrc << 'EOF'
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
EOF

source ~/.bashrc

# 最小配置
echo 'export JAVA\_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> $HADOOP\_CONF\_DIR/hadoop-env.sh
cat > $HADOOP\_CONF\_DIR/core-site.xml <<'EOF'
<configuration>
<property>
<name>fs.defaultFS</name>
<value>hdfs://master:9000</value>
</property>
</configuration>
EOF

cat > $HADOOP\_CONF\_DIR/hdfs-site.xml <<'EOF'
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

cp $HADOOP\_CONF\_DIR/mapred-site.xml.template $HADOOP\_CONF\_DIR/mapred-site.xml
cat > $HADOOP\_CONF\_DIR/mapred-site.xml <<'EOF'
<configuration>
<property>
<name>mapreduce.framework.name</name>
<value>yarn</value>
</property>
</configuration>
EOF

cat > $HADOOP\_CONF\_DIR/yarn-site.xml <<'EOF'
<configuration>
<property>
<name>yarn.resourcemanager.hostname</name>
<value>master</value>
</property>
<property>
<name>yarn.nodemanager.aux-services</name>
<value>mapreduce\_shuffle</value>
</property>
</configuration>
EOF

# 让master 也作为 worker
echo -e "master\\nworker1" > $HADOOP\_CONF\_DIR/workers

# 同步到 worker1 
cd ~
rsync -av $HADOOP_HOME/etc/hadoop/ hadoop@worker1:$HADOOP_HOME/etc/hadoop/
rsync -av ~/.bashrc hadoop@worker1:/home/hadoop/

# 不知道为啥之后发现worker1环境变量还是没配置好，又手动加了PATH
nano ~/.bashrc
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
source ~/.bashrc

# 在 master 上创建日志目录 (因为之前数据目录配置好了)
sudo mkdir -p /usr/local/hadoop/logs

sudo chown hadoop:hadoop /usr/local/hadoop/logs
sudo chmod 750 /usr/local/hadoop/logs

# 在 worker1 上创建数据目录 & 日志目录
# 数据目录
sudo mkdir -p /usr/local/hadoop/data/namenode
sudo mkdir -p /usr/local/hadoop/data/datanode
# 日志目录
sudo mkdir -p /usr/local/hadoop/logs

sudo chown -R hadoop:hadoop /usr/local/hadoop/data
sudo chown hadoop:hadoop /usr/local/hadoop/logs

sudo chmod -R 750 /usr/local/hadoop/data
sudo chmod 750 /usr/local/hadoop/logs


# 初始化启动
hdfs namenode -format
start-dfs.sh
start-yarn.sh

# 验证
jps # 列出本机正在运行的Java进程 
ssh worker1 'jps' # 从 master 远程到 worker1 上执行 jps
hdfs dfs -mkdir -p /user/$(whoami) # 在 HDFS 里建家目录 /user/hadoop
echo "hi" > /tmp/hello.txt # 在本机的 Linux 文件系统里生成一个小文件
hdfs dfs -put /tmp/hello.txt # 把这个本地文件上传到 HDFS 根目录
hdfs dfs -ls # 列出 HDFS 根目录内容
hdfs dfs -cat /hello.txt #从 HDFS 读取文件并打印到终端，应该输出 hi
```

7. 再次检查是否与老师设置对齐
```bash
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
[[ "$rep" == "2" ]] && ok "dfs.replication=$rep" || fail "dfs.replication=$rep（应为 2）"
nn=$(hdfs getconf -confKey dfs.namenode.name.dir 2>/dev/null || true)
[[ "$nn" == "file:///usr/local/hadoop/data/namenode" ]] && ok "dfs.namenode.name.dir=$nn" || fail "namenode.dir=$nn（应为 file:///usr/local/hadoop/data/namenode）"
dn=$(hdfs getconf -confKey dfs.datanode.data.dir 2>/dev/null || true)
[[ "$dn" == "file:///usr/local/hadoop/data/datanode" ]] && ok "dfs.datanode.data.dir=$dn" || fail "datanode.dir=$dn（应为 file:///usr/local/hadoop/data/datanode）"

echo "== 3) YARN / MapReduce 关键参数（grep 方式校验） =="
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

chmod +x ~/check_hadoop.sh
bash ~/check_hadoop.sh
```
---
## 四：启动Hadoop并创建目录

1. 启动：VM 重启后 Hadoop 不会自动启动
```bash
sudo -i -u hadoop # 到Hadoop下执行
source ~/.bashrc # 确保环境变量生效

# 启动 HDFS 和 YARN（会顺带启动 worker1）
start-dfs.sh
start-yarn.sh

# 再手动启动一下datanode，namenode （有时候自动起不起来）
hdfs datanode
hdfs namenode

# 验证
jps #本机应见 NameNode/ResourceManager
ssh worker1 'jps' #worker1 应见 DataNode/NodeManager        
```

在网页中打开Hadoop
```bash
ssh -NT -L 8088:10.0.0.36:8088 adm-mcsc@esilv-mcscin5a1825-0030.westeurope.cloudapp.azure.com

# 打开浏览器
# http://localhost:8088

## start-dfs脚本出问题时启动流程
# 1. master 上，以 hadoop 用户
sudo -i -u hadoop
source ~/.bashrc

# HDFS 守护进程
hdfs --daemon start namenode
hdfs --daemon start datanode

# 2. worker1 上，以 hadoop 用户
sudo -i -u hadoop
source ~/.bashrc
hdfs --daemon start datanode

# 3. 回 master 上，启动 YARN
start-yarn.sh

# 4. 验证
jps
ssh worker1 'jps'

```

2. 创建目录
```bash
# 在 master 上执行
hdfs dfs -mkdir -p /data/weather/raw
hdfs dfs -mkdir -p /checkpoints/weather_raw
```
