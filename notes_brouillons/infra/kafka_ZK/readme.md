# Kafka / ZooKeepe 集群脚本说明

本目录存放的是 **在两台 VM 上启动 Kafka / ZooKeepe 的脚本和说明**。  
**！！权限没设置好，脚本暂时跑不起来，之后改一下**

集群拓扑如下：

- `master`：Zookeeper + Kafka broker #1
- `worker1`：Kafka broker #2

> 约定：所有 Kafka 相关进程使用 `kafka` 用户运行\
> Kafka 安装目录为 `/opt/kafka`

## 1. 前置条件（两台VM相同）

1. 已创建 `kafka` 用户, 并能 `sudo -i -u kafka`

2. **数据目录:** 
    - broker #1 -> `/data/kafka/broker_1` 
    - broker #2 -> `/data/kafka/broker_2` 

   **日志目录:**
    - broker #1 -> `/var/log/kafka_1` 
    - broker #2 -> `/var/log/kafka_2` 
    - zookeeper -> `/opt/kafka/zk-data`

## 2. 文件列表
 - `setup_kafka_master.sh`\
在 master 上运行(adm-mcsc用户), 创建目录，修改权限，固定目录
 - `setup_kafka_worker1.sh`\
在 worker1 上运行(adm-mcsc用户), 创建目录，修改权限，固定目录
 - `start_zk_master.sh`\
在 master 上运行(kafka用户), 启动ZK
 - `start_kafka_master.sh`\
在 master 上运行(kafka用户), 启动Kafka broker #1
 - `start_kafka_worker1.sh`\
在 worker1 上运行(kafka用户), 启动Kafka broker #2
 - `create_topic_weather.sh`\
在 master 上运行(kafka用户), 创建topic

## 3. 脚本运行顺序
1. 只在第一次搭集群时: 
- 在 master 上，以 adm-mcsc 用户运行`setup_kafka_master.sh` 
```bash
cd ~/projet_esilv/BigData/Iceland_Short-Term Wind Forecast_Extreme Wind Alerts/initial skeleton/infra/kafka_ZK

bash setup_kafka_master.sh
```
- 在 worker1 上，以 adm-mcsc 用户运行`setup_kafka_worker1.sh`
```bash
cd ~/projet_esilv/BigData/Iceland_Short-Term Wind Forecast_Extreme Wind Alerts/initial skeleton/infra/kafka_ZK

bash setup_kafka_worker1.sh
```

2. 每次开机之后启动集群时：
- 在 master 上，以 kafka 用户运行 `start_zk_master.sh`
```bash
sudo -i -u kafka
cd ~/projet_esilv/BigData/Iceland_Short-Term Wind Forecast_Extreme Wind Alerts/initial skeleton/infra/kafka_ZK

bash start_zk_master.sh
```
- 在 master 上，以 kafka 用户运行 `start_kafka_master.sh` <- 另起一个终端
```bash
sudo -i -u kafka
cd ~/projet_esilv/BigData/Iceland_Short-Term Wind Forecast_Extreme Wind Alerts/initial skeleton/infra/kafka_ZK

bash start_kafka_master.sh
```
- 在 worker1 上，以 kafka 用户运行 `start_kafka_worker1.sh`
```bash
sudo -i -u kafka
cd ~/projet_esilv/BigData/Iceland_Short-Term Wind Forecast_Extreme Wind Alerts/initial skeleton/infra/kafka_ZK

bash start_kafka_worker1.sh
```

3. 创建 `weather_iceland_raw` topic (在 master 上，以 kafka 用户运行):
```bash
sudo -i -u kafka
cd ~/projet_esilv/BigData/Iceland_Short-Term Wind Forecast_Extreme Wind Alerts/initial skeleton/infra/kafka_ZK

bash create_topic_weather.sh
```

## 4. 与 Hadoop / Spark 的衔接
- **Kafka 的 `bootstrap-server`:** `master:9092,worker1:9092`

- Spark Structured Streaming 读取 `topic weather_iceland_raw`\
写入 HDFS：`/data/weather/raw`
checkpoint：`/checkpoints/weather_raw`