# 启动 Spark Streaming（Kafka → HDFS）

## 一：流程
1. 复制 `spark_streaming_job.py` 到hadoop的home
```bash
# 因为stream和真个project文件都被我放在adm里面，而且hadoop权限不够，所以要通过adm复制文件到hadoop的home
# 到adm
cd ~
ls streaming   # 确认目录在
# 把整个 streaming 目录复制到 hadoop 的 home 下
sudo cp -r ~/streaming /home/hadoop/streaming_from_adm

# 把所有权改成 hadoop（否则 hadoop 虽然能进目录，但可能没写权限）
sudo chown -R hadoop:hadoop /home/hadoop/streaming_from_adm

2. 运行spark-submit
```bash
sudo -i -u hadoop          # 切换身份
cd ~/streaming_from_adm    # 这就是 /home/hadoop/streaming_from_adm
ls                         # 这里应该能看到 spark_streaming_job.py

/opt/spark/bin/spark-submit --master yarn --deploy-mode client --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 spark_streaming_job.py

3. 测试数据能否落地HDFS
- 回到终端 `kafka@master`
```bash
sudo -i -u kafka
cd /opt/kafka

bin/kafka-console-producer.sh \
  --broker-list master:9092,worker1:9093 \
  --topic weather_iceland_raw

# input：
# reykjavik,2025-11-27T05:00Z,10.5
# akureyri,2025-11-27T05:00Z,7.2
# keflavik,2025-11-27T05:00Z,12.0
```
- 回到终端 `hadoop@master`
```bash
sudo -i -u hadoop
source ~/.bashrc

hdfs dfs -ls /data/weather/raw
# 看是否有文件输出
```