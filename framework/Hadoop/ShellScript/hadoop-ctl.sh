#!/bin/bash
# 用于启动Hadoop集群,包括HDFS/YARN/JobHistoryServer

# 判断参数个数
if (($# != 1)); then
    echo -e "\nWorng Parameter!"
    exit 1
fi

# 获取当前时间(相对时间)
start_time=$(date +%s)
# 获取操作方式
operate=$1
# 设定HDFS客户端,即NameNode节点地址
HDFS_Client="hadoop101"
# 设定YARN客户端,即ResourceManager节点地址
YARN_Client="hadoop102"
# 设置任务历史服务器客户端,即JobHistoryServer节点地址
JobHistoryServer="hadoop103"
# 指定启动用户
user="tomandersen"

case $operate in
start)
    # 启动HDFS集群
    echo -e "\n----------Starting HDFS cluster----------"
    ssh $user@$HDFS_Client "source /etc/profile;
    HADOOP_HOME=${HADOOP_HOME:-/opt/module/hadoop-2.7.7};
    $HADOOP_HOME/sbin/start-dfs.sh"
    # 启动YARN集群
    echo -e "\n----------Starting YARN cluster----------"
    ssh $user@$YARN_Client "source /etc/profile;
    HADOOP_HOME=${HADOOP_HOME:-/opt/module/hadoop-2.7.7};
    $HADOOP_HOME/sbin/start-yarn.sh"
    # 启动历史服务器
    echo -e "\n----------Starting JobHistoryServer----------"
    ssh $user@$JobHistoryServer "source /etc/profile;
    HADOOP_HOME=${HADOOP_HOME:-/opt/module/hadoop-2.7.7};
    $HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver"
    ;;
stop)
    # 关闭HDFS集群
    echo -e "\n----------Stopping HDFS cluster----------"
    ssh $user@$HDFS_Client "source /etc/profile;
    HADOOP_HOME=${HADOOP_HOME:-/opt/module/hadoop-2.7.7};
    $HADOOP_HOME/sbin/stop-dfs.sh"
    # 关闭YARN集群
    echo -e "\n----------Stopping YARN cluster----------"
    ssh $user@$YARN_Client "source /etc/profile;
    HADOOP_HOME=${HADOOP_HOME:-/opt/module/hadoop-2.7.7};
    $HADOOP_HOME/sbin/stop-yarn.sh"
    # 关闭历史服务器
    echo -e "\n----------Stopping JobHistoryServer----------"
    ssh $user@$JobHistoryServer "source /etc/profile;
    HADOOP_HOME=${HADOOP_HOME:-/opt/module/hadoop-2.7.7};
    $HADOOP_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver"
    ;;
*)
    echo -e "\nWorng Parameter!"
    exit 1
    ;;
esac

end_time=$(date +%s)
execution_time=$((${end_time} - ${start_time}))
echo -e "\n----------$operate Hadoop cluster takes ${execution_time} seconds----------\n"
