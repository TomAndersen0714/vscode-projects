#!/bin/bash
# 此脚本只用于控制HDFS HA集群的DataNode进程

# 判断参数个数
if (($# != 1)); then
    echo -e "\nWorng Parameter!"
    exit 1
fi

# 获取当前时间(相对时间)
start_time=$(date +%s)
# 获取操作方式
operate=$1
# 设置HA模式下的DataNode集群
cluster=${CLUSTER:-"hadoop101 hadoop102 hadoop103"}

# 遍历集群,启动DataNode
case $operate in
start)
    echo -e "\n----------Starting the hdfs HA datanode cluster----------"
    for host in $cluster; do
        ssh $host "source /etc/profile;
        HADOOP_HA_HOME=${HADOOP_HA_HOME:-/opt/module/HA/hadoop-2.7.7};
        $HADOOP_HA_HOME/sbin/hadoop-daemon.sh start datanode;"
    done
    ;;
stop)
    echo -e "\n----------Stopping the hdfs HA datanode cluster----------"
    for host in $cluster; do
        ssh $host "source /etc/profile;
        HADOOP_HA_HOME=${HADOOP_HA_HOME:-/opt/module/HA/hadoop-2.7.7};
        $HADOOP_HA_HOME/sbin/hadoop-daemon.sh stop datanode;"
    done
    ;;
*)
    echo -e "\nWorng Parameter!"
    exit 1
    ;;
esac

# 计算结束时间
end_time=$(date +%s)
execution_time=$((${end_time} - ${start_time}))
echo -e "\n----------$operate HDFS HA datanodes cluster takes ${execution_time} seconds----------\n"
