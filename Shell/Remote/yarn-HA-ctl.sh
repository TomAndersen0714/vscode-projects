#!/bin/bash
# 用于启动和关闭YARN HA模式,前提是启动了Zookeeper集群
# 第1个参数为操作方式start/stop,第2个参数为指定成Active的RM节点ip

# 判断参数个数
if (($# != 2 && $# != 1)); then
    echo "Wrong Parameters"
    exit 1
fi

# 获取当前时间(相对时间)
start_time=$(date +%s)
# ResourceManager HA集群设置
cluster="hadoop101 hadoop102"
# 默认设置hadoop102为Active RM
activeRM=${2:-hadoop102}
# 将Active RM从RM集群中剔除,便于后续进行不同操作
cluster=$(echo $cluster | sed "s/$activeRM//")

# 获取操作方式
operate=$1
# HADOOP_HA_HOME设置默认值
HADOOP_HA_HOME=${HADOOP_HA_HOME:-/opt/module/HA/hadoop-2.7.7}

# 调用YARN脚本
case $operate in
start)
    # 启动YARN HA集群
    echo -e "\n----------Starting the yarn HA cluster----------"
    for host in $cluster; do
        ssh $host "source /etc/profile;
        HADOOP_HA_HOME=${HADOOP_HA_HOME:-/opt/module/HA/hadoop-2.7.7};
        $HADOOP_HA_HOME/sbin/yarn-daemon.sh start resourcemanager;"
    done
    # 后启动的RM为Active
    ssh $activeRM "source /etc/profile;
        HADOOP_HA_HOME=${HADOOP_HA_HOME:-/opt/module/HA/hadoop-2.7.7};
        $HADOOP_HA_HOME/sbin/yarn-daemon.sh start resourcemanager;"
    # 启动NM集群
    yarn-HA-nodemanagers-ctl.sh start
    ;;
stop)
    # 关闭YARN HA集群
    echo -e "\n----------Stopping the yarn HA cluster----------"
    for host in $cluster; do
        ssh $host "source /etc/profile;
        HADOOP_HA_HOME=${HADOOP_HA_HOME:-/opt/module/HA/hadoop-2.7.7};
        $HADOOP_HA_HOME/sbin/yarn-daemon.sh stop resourcemanager;"
    done
    # 最后关闭Active,若提前关闭会进行Automatic Failover
    ssh $activeRM "source /etc/profile;
        HADOOP_HA_HOME=${HADOOP_HA_HOME:-/opt/module/HA/hadoop-2.7.7};
        $HADOOP_HA_HOME/sbin/yarn-daemon.sh stop resourcemanager;"
    # 关闭NM集群
    yarn-HA-nodemanagers-ctl.sh stop
    ;;
*)
    echo -e "\nWorng Parameter"
    exit 1
    ;;
esac

end_time=$(date +%s)
execution_time=$((${end_time} - ${start_time}))
echo -e "\n----------$operate YARN HA cluster takes ${execution_time} seconds----------\n"
