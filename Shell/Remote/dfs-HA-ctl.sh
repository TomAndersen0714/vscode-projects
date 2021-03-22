#!/bin/bash
# 用于启动和关闭HDFS HA模式,前提是启动了Zookeeper集群

# 判断参数个数
if (($# != 1)); then
    echo -e "\nWorng Parameter"
    exit 1
fi
# 获取当前时间(相对时间)
start_time=$(date +%s)

# JobHistoryServer节点设置
job_history_server="hadoop103"

# 获取操作方式
operate=$1
HADOOP_HA_HOME=${HADOOP_HA_HOME:-/opt/module/HA/hadoop-2.7.7}
# 调用HDFS脚本
case $operate in
start)
    echo -e "\n----------Starting the hdfs HA cluster----------"
    $HADOOP_HA_HOME/sbin/start-dfs.sh
    # 启动HA集群任务历史服务器
    echo -e "\n----------Starting JobHistoryServer----------"
    ssh $job_history_server "source /etc/profile;
    $HADOOP_HA_HOME/sbin/mr-jobhistory-daemon.sh start historyserver"
    ;;
stop)
    echo -e "\n----------Stopping the hdfs HA cluster----------"
    $HADOOP_HA_HOME/sbin/stop-dfs.sh
    # 关闭HA集群任务历史服务器
    echo -e "\n----------Stopping JobHistoryServer----------"
    ssh $job_history_server "source /etc/profile;
    $HADOOP_HA_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver"
    ;;
*)
    echo -e "\nWorng Parameter"
    exit 1
    ;;
esac

# 获取结束时间
end_time=$(date +%s)
execution_time=$((${end_time} - ${start_time}))
echo -e "\n----------$operate HDFS HA cluster takes ${execution_time} seconds----------\n"
