#!/bin/bash
# 用于启动Hadoop HA模式,包括(NameNode HA和ResourceManager HA)

# 判断参数个数
if (($# != 1)); then
    echo -e "\nWorng Parameter"
    exit 1
fi

# 获取当前时间(相对时间)
start_time=$(date +%s)
# 获取操作方式
operate=$1
case $operate in
start)
    # 启动zookeeper集群
    zkCluster-ctl.sh start
    if (($? != 0)); then
        echo -e "\n----------Failed to start zookeeper cluster----------"
        exit 1
    fi
    # 启动HDFS HA集群
    dfs-HA-ctl.sh start
    # 启动YARN HA集群
    yarn-HA-ctl.sh start
    ;;
stop)
    # 关闭HDFS HA集群
    dfs-HA-ctl.sh stop
    # 关闭YARN HA集群
    yarn-HA-ctl.sh stop
    ;;
*)
    echo -e "\nWorng Parameter"
    exit 1
    ;;
esac

end_time=$(date +%s)
execution_time=$((${end_time} - ${start_time}))
echo -e "\n----------$operate Hadoop HA cluster takes ${execution_time} seconds----------\n"
