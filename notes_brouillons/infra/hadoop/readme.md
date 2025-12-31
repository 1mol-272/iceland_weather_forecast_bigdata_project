# Hadoop 集群脚本说明

本目录存放的是 **在两台 VM 上启动 Hadoop 的脚本和说明**。  
**！！权限没设置好，脚本暂时跑不起来，之后改一下**

集群拓扑如下：

- `master`：NameNode + DataNode + ResourceManager 
- `worker1`：DataNode + NodeManager

> 约定：所有 Hadoop 相关进程都用 **`hadoop` 用户** 运行\
> Hadoop 安装在 `/usr/local/hadoop`，配置目录为 `/usr/local/hadoop/etc/hadoop`

---

## 1. 前置条件（两台VM相同）

1. 已创建 `hadoop` 用户，并能 `sudo -i -u hadoop`。

2. 环境变量（在 `hadoop` 用户的 `~/.bashrc` 中）：

   ```bash
   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
   export HADOOP_HOME=/usr/local/hadoop
   export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
   export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
   ```

3. **数据目录:** `/usr/local/hadoop/data/namenode` `/usr/local/hadoop/data/datanode`
   **日志目录:** `/usr/local/hadoop/logs`

4. 配置文件（$HADOOP_CONF_DIR）中已设置好：

 - `fs.defaultFS = hdfs://master:9000`
 - `dfs.namenode.name.dir = file:///usr/local/hadoop/data/namenode`
 - `dfs.datanode.data.dir = file:///usr/local/hadoop/data/datanode`
 - `yarn.resourcemanager.hostname = master`
 - workers 文件包含：`master` `worker1`

## 2. 文件列表
- `start_hadoop_master.sh`\
在 master 上运行(hadoop用户)，启动 NameNode + DataNode + YARN
- `start_datanode_worker1.sh`\
在 worker1 上运行(hadoop用户)，启动 DataNode
- `setup_hadoop_config.sh`\
在 master 上运行(hadoop用户), 配置环境变量并同步到 worker1
- `check_hadoop_settings.sh`
检查hadoop配置是否与老师对齐(adm-mcsc用户）

## 3. 脚本运行顺序
1. **只在第一次搭集群时: `setup_hadoop_config.sh`**

```bash
# 在master上，切到hadoop用户，进入脚本目录并执行：
sudo -i -u hadoop
cd ~/projet_esilv/BigData/'Iceland_Short-Term Wind Forecast_Extreme Wind Alerts'/'initial skeleton'/infra/hadoop   

bash setup_hadoop_config.sh

source ~/.bashrc
```

2. 只在第一次 format 时: 运行`hdfs namenode -format`
```bash
# 在master上，切到hadoop用户, 进行format
sudo -i -u hadoop
source ~/.bashrc

hdfs namenode -format
```

3. 每次开机之后启动集群时：
- 在 master 上运行 `start_hadoop_master.sh`（可以用 adm-mcsc 用户）
   ```bash
   cd ~/projet_esilv/BigData/'Iceland_Short-Term Wind Forecast_Extreme Wind Alerts'/'initial skeleton'/infra/hadoop 

   bash start_hadoop_master.sh
   ```
- 在 worker1 上运行 `start_datanode_worker1.sh`（可以用 adm-mcsc 用户）
   ```bash
   cd ~/projet_esilv/BigData/'Iceland_Short-Term Wind Forecast_Extreme Wind Alerts'/'initial skeleton'/infra/hadoop 

   bash start_datanode_worker1.sh
   ```

4. 如有需要，用 `check_hadoop_settings.sh` 做健康检查 `setup_hadoop_config.sh`
```bash
# 在master上，切到hadoop用户
sudo -i -u hadoop
source ~/.bashrc
cd ~/projet_esilv/BigData/'Iceland_Short-Term Wind Forecast_Extreme Wind Alerts'/'initial skeleton'/infra/hadoop  

bash check_hadoop_settings.sh
```

