# Cluster plan (2 VMs)

## VM1 (master-ish)
- HDFS NameNode
- Spark Master / Driver
- Kafka Broker
- Time-series DB (TimescaleDB or InfluxDB)
- Grafana

## VM2 (worker)
- HDFS DataNode
- Spark Worker / Executor