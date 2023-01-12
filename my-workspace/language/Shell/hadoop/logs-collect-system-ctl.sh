#!/bin/bash
# 此脚本只允许接收strat/stop参数
# 此脚本用于启动Zookeeper集群、Hadoop普通集群、Kafka集群、Flume Agent形成的
# 日志采集系统,启动顺序为Zookeeper->HDFS->YARN->Jobhistory->Kafka->flume Agent

# 判断参数个数
if (($# != 1)); then
    echo -e "\nWrong parameters!" >&2
    exit 1
fi
# 获取操作模式
operation=$1
# 获取当前时间(相对时间)
start_time=$(date +%s)

# 根据操作模式启动/关闭日志采集系统
case "$operation" in
"start")
    {
        # 启动日志采集系统
        echo -e "\n----------Starting logs collection system----------"
        # 启动Zookeeper集群
        zkCluster-ctl.sh start &&
        # 启动Hadoop集群
        hadoop-ctl.sh start &&
        # 启动Kafka集群
        kafkaCluster-ctl.sh start &&
        # 启动Flume Agent开始收集日志
        collect-logs-ctl.sh start &&
        # 启动Flume Agent开始消费Kafka日志到HDFS
        transport-logs-ctl.sh start
    }
    ;;

"stop")
    {
        # 关闭日志采集系统
        echo -e "\n----------Stopping logs collection system----------"
        # 关闭Flume Agent日志采集
        collect-logs-ctl.sh stop &&
        # 关闭Flume Agent日志消费
        transport-logs-ctl.sh stop
        # 关闭Kafka集群
        kafkaCluster-ctl.sh stop &&
        # 关闭Hadoop集群
        hadoop-ctl.sh stop &&
        # 关闭Zookeeper集群
        zkCluster-ctl.sh stop
    }
    ;;

*)
    {
        # 输出的参数模式错误
        echo -e "\nWrong parameters!" >&2
        exit 1 
    }
    ;;
esac

# 获取当前时间
end_time=$(date +%s)
# 计算脚本运行时间
execution_time=$(($end_time - $start_time))
echo -e "\n----------$operation logs collection system takes $execution_time seconds----------"
