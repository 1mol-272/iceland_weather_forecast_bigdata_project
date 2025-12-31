# 启动Hadoop，kafka

## 一：总体框架

**VM1（master）：** NameNode + DataNode + ResourceManager 
**VM1（master）：** DataNode + NodeManager

**VM1（master）：** ZK + broker1
**VM1（master）：** broker2

---

## 二：流程

### 链接虚拟机
ssh adm-mcsc@esilv-mcscin5a1825-0030.westeurope.cloudapp.azure.com\
ssh adm-mcsc@esilv-mcscin5a1825-0031.westeurope.cloudapp.azure.com

1. 登录jupyter网页（我一般master在网页上跑，worker1在本机跑）
```bash
source /opt/spark/venv/bin/activate

jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --notebook-dir=/

# 新开一个终端，不要关
ssh -L 8888:localhost:8888 adm-mcsc@esilv-mcscin5a1825-0030.westeurope.cloudapp.azure.com

# http://localhost:8888/ 使用输出的token登录
```

### 启动hadoop
- 在VM1上
1. 以hadoop用户启动datanode和namenode
```bash
sudo -i -u hadoop 
# 报错了就去重设nano
source ~/.bashrc
hdfs --daemon start namenode
hdfs --daemon start datanode
start-yarn.sh
jps
ssh worker1 'jps'
```

2. 建目录
```bash
hdfs dfs -mkdir -p /data/weather/raw
hdfs dfs -mkdir -p /checkpoints/weather_raw
```

-在VM2上
3. 以hadoop用户启动datanode
```bash
sudo -i -u hadoop 
source ~/.bashrc
hdfs --daemon start datanode
jps
```

### 启动kafka
- 在VM1上
1. 以kafka用户启动Zookeeper
```bash
sudo su - kafka
cd /opt/kafka
# 前台启动 -> 方便查看日志
bin/zookeeper-server-start.sh config/zookeeper.properties
# 后台启动
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties

# 验证
ss -tnlp | grep 2181 # 看 2181 端口是否在监听（Ubuntu 24 用 ss）
```

2. 为了让VM1连接上ZK，为ZM做一些基本配置
```bash
# 另启一个终端
ssh adm-mcsc@esilv-mcscin5a1825-0030.westeurope.cloudapp.azure.com

# 基础配置，告诉这个 Kafka「我是谁、在哪听、连哪个 ZK」
# 直接追加到最后 -> 不去大范围改原始文件，而是直接在末尾追加“custom seting”
# 同一个key，解析是以后后面的为准
cd /opt/kafka
sudo tee -a config/server.properties << 'EOF'

######## custom settings for our cluster ########
broker.id=1 # 这个kafka节点的“身份证号”
listeners=PLAINTEXT://master:9092 # kafka实际监听的地址和端口 -> 在master：9092这个地址开了个TCP端口
advertised.listeners=PLAINTEXT://master:9092 # kafka告诉客户端的地址 -> 其他机器想连这台kafka，要能解析/访问这个地址
zookeeper.connect=master:2181 # kafka需要去哪儿找zookeeper -> kafka启动时会用这个地址去注册自己，读写元数据（toppic信息，分区分配等）
EOF
```

3. 在VM1上启动Kafka broker，连接ZK
```bash
# 切到kafka用户，启动kafka broker
sudo su - kafka
cd /opt/kafka

# 如果跑挂了可以启动前，在当前shell里给kafka限制一个小一点的堆(heap)
export KAFKA_HEAP_OPTS="-Xms256m -Xmx256m"
bin/kafka-server-start.sh config/server.properties
# 保持这个终端一直开着

# 在另一个终端验证一下
ss -tnlp | grep 9092
ps aux | grep kafka.Kafka | grep -v grep
```

- 在VM2上
4. 为了让VM2连接上ZK，为VM做一些基本配置
```bash
ssh adm-mcsc@esilv-mcscin5a1825-0031.westeurope.cloudapp.azure.com

cd /opt/kafka
sudo tee -a config/server.properties << 'EOF'

######## custom settings for our cluster ########
broker.id=2 
listeners=PLAINTEXT://master:9093 
advertised.listeners=PLAINTEXT://master:9093 
zookeeper.connect=master:2181 （
EOF
```

5. 在VM2上启动Kafka broker，连接ZK
```bash
# 切到kafka用户，启动kafka broker
sudo su - kafka
cd /opt/kafka

bin/kafka-server-start.sh config/server.properties
```

6. 在 VM1 上验证现在集群有 2 个 broker
```bash
sudo su - kafka
cd /opt/kafka

# 看一下集群里的 broker 列表
bin/zookeeper-shell.sh master:2181 <<< "ls /brokers/ids"
```

