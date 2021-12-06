#!/bin/bash
# 此脚本只用于控制YARN HA集群的NodeManager进程

# 判断参数个数
if (($# != 1)); then
    echo -e "\nWorng Parameter!"
    exit 1
fi

# 获取当前时间(相对时间)
start_time=$(date +%s)
# 获取操作方式
operate=$1
# 设置HA模式下的NodeManager集群
cluster=${CLUSTER:-"hadoop101 hadoop102 hadoop103"}

# 遍历集群,启动NodeManager
case $operate in
start)
    echo -e "\n----------Starting the YARN HA NodeManager cluster----------"
    for host in $cluster; do
        ssh $host "source /etc/profile;
        HADOOP_HA_HOME=${HADOOP_HA_HOME:-/opt/module/HA/hadoop-2.7.7};
        $HADOOP_HA_HOME/sbin/yarn-daemon.sh start nodemanager"
    done
    ;;
stop)
    echo -e "\n----------Stopping the YARN HA NodeManager cluster----------"
    for host in $cluster; do
        ssh $host "source /etc/profile;
        HADOOP_HA_HOME=${HADOOP_HA_HOME:-/opt/module/HA/hadoop-2.7.7};
        $HADOOP_HA_HOME/sbin/yarn-daemon.sh stop nodemanager"
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
echo -e "\n----------$operate YARN HA NodeManager cluster takes ${execution_time} seconds----------\n"
