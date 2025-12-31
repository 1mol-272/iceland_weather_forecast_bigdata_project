# kafka（broker + zookeeper） 总体框架 + 配置 （创建目录 + 设置权限）

## 一：总体框架

**VM1（master）：** zookeeper (数据目录) + kafka (数据目录+日志目录)

**VM2（worker1）：** kafka (数据目录+日志目录)

---

## 二： 配置 

1. 下载VM
```bash
cd /tmp

# 1. 下载 Kafka 安装包
sudo wget https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz

# 2. 创建 /opt 并解压到 /opt
sudo mkdir -p /opt
sudo tar xzf kafka_2.13-3.7.0.tgz -C /opt

# 3. 把解压出来的目录统一命名为 /opt/kafka
sudo mv /opt/kafka_2.13-3.7.0 /opt/kafka
```

2. 创建`kafka`用户
```bash
sudo useradd -m -s /bin/bash kafka
id kafka # 确认创建成功
# output： uid=1002(kafka) gid=1002(kafka) groups=1002(kafka)
```

3. 把`/opt/kafka`程序目录收回给root管: 安装包、配置文件所在由root:root 管理，Kafka 进程只读
```bash
sudo chown -R root:root /opt/kafka
sudo chmod -R 755 /opt/kafka
# cbown: change owner
# -R: 递归
# root:root：用户:用户组
# 把 /opt/kafka 及其所有子内容的所有者和属组都改成 root:root
```
*补充 i : 数字权限法*
```bash
第一位：user
第二位：group
第三位：others

每一位数字是r(读), w(写) x(执行)的相加：
7 = 4+2+1 = rwx
6 = 4+2 = rw-
5 = 4+1 = r-x
4 = 4 = r--
0 0 = ---

ex:
755 → 拥有者 rwx，别人 r-x → 常用于目录、可执行程序
644 → 拥有者 rw-，别人 r-- → 常用于普通配置文件、文本文件
600 → 拥有者 rw-，别人没权限 → 私密文件（如密钥）
```

4. 给`kafka`准备<日志目录(`/var/log/kafka`) + 数据目录（`/data/kafka/logs`)>

日志目录: 用来存`server.log`等，必须`kafka`能写\
数据目录: 即`broker`目录， 用来存topic/分区/消息文件，必须`kafka`能写
```bash
# 程序日志目录， 并设置权限
sudo mkdir -p /var/log/kafka
sudo chown kafka:kafka /var/log/kafka
sudo chmod 750 /var/log/kafka  

# 数据目录， 并设置权限
sudo mkdir -p /data/kafka/broker
sudo chown -R kafka:kafka /data/kafka
sudo chmod 700 /data/kafka/broker

# 固定日志目录
cd /opt/kafka
sudo sed -i 's|LOG_DIR="$base_dir/logs"|LOG_DIR=${LOG_DIR:-"/var/log/kafka"}|' bin/kafka-run-class.sh
grep -n 'LOG_DIR=' bin/kafka-run-class.sh 确认目录存在

#固定数据目录
cd /opt/kafka
grep -n '^log.dirs' config/server.properties # 检查有没有log.dirs这一行
sudo sed -i 's|^log.dirs=.*|log.dirs=/data/kafka/broker|' config/server.properties #改目录
grep -n '^log.dirs' config/server.properties 确认目录存在
```

5. 给`zookeeper`准备<数据目录> <--- 仅在master上
```bash
# 创建数据目录， 并设置权限
sudo mkdir -p /opt/kafka/zk-data
sudo chown kafka:kafka /opt/kafka/zk-data
sudo chmod 700 /opt/kafka/zk-data
ls -ld /opt/kafka/zk-data # 确认一下

# 固定数据目录
cd /opt/kafka
grep -n '^dataDir' config/zookeeper.properties
sudo sed -i 's|^dataDir=.*|dataDir=/opt/kafka/zk-data|' config/zookeeper.properties
grep -n '^dataDir' config/zookeeper.properties

