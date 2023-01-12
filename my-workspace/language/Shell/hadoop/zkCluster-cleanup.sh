#!/bin/bash
# 此脚本用于格式化zkServer

# 获取当前时间(相对时间)
start_time=$(date +%s)
# 设置zkCluster
zkCluster=${zkCluster:-"zkServer1 zkServer2 zkServer3"}
# 设置zookeeper路径
ZOOKEEPER_HOME=${ZOOKEEPER_HOME:-"/opt/module/zookeeper-3.4.14"}
# 设置zookeeper本地数据存储相对路径
zkdata_dir="zkData"
# 设置zookeeper本地事务日志和运行日志存储相对路径
zk_logs="logs"

# 遍历zkCluster,删除所有节点上的数据和日志
echo -e "\n----------Formatting the Zookeeper cluster----------"
for host in $zkCluster; do
    echo -e "\n----------Formatting the $host----------"
    ssh $host "source /etc/profile;
    find $ZOOKEEPER_HOME/${zkdata_dir} -mindepth 1 -maxdepth 1 |grep -v "myid" |xargs rm -rf
    find $ZOOKEEPER_HOME/${zk_logs} -mindepth 1 -maxdepth 1 |xargs rm -rf
    "
done

if (($? != 0)); then
    echo -e "\n----------Failed to format Zookeeper cluster----------"
fi

# 获取结束时间
end_time=$(date +%s)
execution_time=$((${end_time} - ${start_time}))
echo -e "\n----------Format the Zookeeper cluster takes ${execution_time} seconds----------\n"
