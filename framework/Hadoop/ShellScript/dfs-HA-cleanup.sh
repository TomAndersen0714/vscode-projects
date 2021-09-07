#!/bin/bash
# 此脚本用于格式化HDFS HA集群,清空HDFS集群中的fsimage/edits/block

# 获取当前时间(相对时间)
start_time=$(date +%s)
# 设置Hadoop HA路径
HADOOP_HA_HOME=${HADOOP_HA_HOME:-"/opt/module/HA/hadoop-2.7.7"}
# 设置fsimage/edits/block存储路径
HADOOP_DATA_DIR="tmp"
# 设置日志路径
HADOOP_LOG_DIR="logs"
# 设置HDFS HA集群
cluster=${CLUSTER:-"hadoop101 hadoop102 hadoop103"}

# 遍历集群,格式化所有主机
echo -e "\n----------Formatting the HDFS HA cluster----------"
for host in $cluster; do
    echo -e "\n----------Formatting the $host----------"
    ssh $host "source /etc/profile;
    find $HADOOP_HA_HOME/$HADOOP_DATA_DIR -mindepth 1 -maxdepth 1|xargs rm -rf;
    find $HADOOP_HA_HOME/$HADOOP_LOG_DIR -mindepth 1 -maxdepth 1|xargs rm -rf;
    "
done

if (($? != 0)); then
    echo -e "\n----------Failed to format HDFS HA cluster----------"
fi

# 获取结束时间
end_time=$(date +%s)
execution_time=$((${end_time} - ${start_time}))
echo -e "\n----------Formatted the HDFS HA cluster takes ${execution_time} seconds----------\n"
