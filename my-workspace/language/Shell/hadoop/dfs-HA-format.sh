#!/bin/bash
# 此脚本用于格式化HDFS HA集群中的NameNode节点
# 前提是配置了zookeeper集群并在Hadoop中配置了zookeeper自动故障转移Automatic Failover
# 并且之前已经将各个节点上hadoop_ha中的的tmp和data文件夹删除,并且zookeeper上也不存在hadoop-ha zkNode
# 只允许接收1个参数,此参数为首个格式化NameNode的ip地址

# 获取当前时间(相对时间)
start_time=$(date +%s)
# 判断参数个数
if (($# != 1)); then
    echo -e "\nWorng Parameter"
    exit 1
fi

# 设置HDFS HA集群NameNode
cluster="hadoop101 hadoop102"
# 尝试启动zookeeper集群
zkCluster-ctl.sh start
if (($? != 0)); then
    echo -e "\n----------Failed to start zookeeper cluster----------"
    exit 1
fi

# 尝试启动journalnode集群
dfs-HA-journalnodes-ctl.sh start
if (($? != 0)); then
    echo -e "\n----------Failed to start journalnode cluster----------"
    exit 1
fi


# 格式化首个NameNode并启动,同时格式化HA zkNode,并启动DFSZKFailoverController
# 此参数为首个格式化NameNode节点
activeNN=$1
echo -e "\n----------Formating the first HA namenodes----------"
ssh $activeNN "source /etc/profile;
HADOOP_HA_HOME=${HADOOP_HA_HOME:-/opt/module/HA/hadoop-2.7.7};
$HADOOP_HA_HOME/bin/hdfs namenode -format;
$HADOOP_HA_HOME/bin/hdfs zkfc -formatZK;
$HADOOP_HA_HOME/sbin/hadoop-daemon.sh start namenode;
$HADOOP_HA_HOME/sbin/hadoop-daemon.sh start zkfc"
if (($? != 0)); then
    echo -e "\n----------Failed to format HA namenode----------"
    exit 1
fi

# 将首个格式化NameNode从集群list中去除,其他NameNode同步格式化并启动
# 同时启动DFSZKFailoverController
echo -e "\n----------Formating the other HA namenodes----------"
cluster=$(echo $cluster | sed "s/$activeNN//")
for host in $cluster; do
    echo -e "\n----------Formating the namenodes $host----------"
    ssh $host "source /etc/profile;
        HADOOP_HA_HOME=${HADOOP_HA_HOME:-/opt/module/HA/hadoop-2.7.7};
        $HADOOP_HA_HOME/bin/hdfs namenode -bootstrapStandby;
        $HADOOP_HA_HOME/sbin/hadoop-daemon.sh start namenode;
        $HADOOP_HA_HOME/sbin/hadoop-daemon.sh start zkfc"
done

if (($? != 0)); then
    echo -e "\n----------Failed to format HA namenode----------"
    exit 1
fi

# 启动DataNode集群
dfs-HA-datanodes-ctl.sh start
if (($? != 0)); then
    echo -e "\n----------Failed to start hdfs HA datanode cluster----------"
    exit 1
fi

end_time=$(date +%s)
execution_time=$((${end_time} - ${start_time}))
echo -e "\n----------Format HDFS HA cluster takes ${execution_time} seconds----------\n"
