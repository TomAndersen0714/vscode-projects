#!/bin/bash
# 此脚本用于启动所有的journalnode

# 判断参数个数
if (($# != 1)); then
    echo "Wrong Parameters"
    exit 1
fi

# 获取当前时间(相对时间)
start_time=$(date +%s)
# 设置journalnode cluster
cluster=${CLUSTER:-"hadoop101 hadoop102 hadoop103"}
# 获取操作方式
operate=$1

case $operate in
start)
    echo -e "\n----------Starting journalnode cluster----------"
    for host in $cluster; do
        echo "----------${host}----------"
        ssh $host "source /etc/profile;$HADOOP_HA_HOME/sbin/hadoop-daemon.sh start journalnode"
    done
    ;;
stop)
    echo -e "\n----------Stopping journalnode cluster----------"
    for host in $cluster; do
        echo "----------${host}----------"
        ssh $host "source /etc/profile;$HADOOP_HA_HOME/sbin/hadoop-daemon.sh stop journalnode"
    done
    ;;
*)
    echo "Wrong Parameter"
    exit 1
    ;;
esac

end_time=$(date +%s)
execution_time=$((${end_time} - ${start_time}))
echo -e "\n----------$operate journalnode takes ${execution_time} seconds----------\n"
