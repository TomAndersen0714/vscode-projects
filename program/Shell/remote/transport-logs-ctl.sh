#!/bin/bash
# 此脚本主要用于启动Flume Agent将Kafka中的启动日志和行为日志消费至HDFS中存储

# 判断参数个数
if (($# != 1)); then
    echo -e "\nWrong Parameters!"
    exit 1
fi
# 获取操作模式参数
operation=$1
# 设置启动Flume Agent的主机集群
# 由于是从Kafka中提取消息,只要是属于同一个Consumer Group就能保证不会消费相同的消息
cluster="hadoop103"
# 获取脚本启动时间
start_time=$(date +%s)
# 设置flume_home目录
flume_home=${FLUME_HOME:-"/opt/module/flume-1.8.0/"}
# 设置flume配置文件目录
flume_conf="conf"
# 设置flume agent配置文件目录
agent_conf="job/kafka-hdfs.properties"
# 设置flume agent名称(要与agent_conf对应的配置文件中的agent名称相对应)
agent_name="a1"

# 根据操作模式参数启动/关闭FLume Agent
case "$operation" in
"start") {
    for host in $cluster; do
        echo -e "\n----------Starting to transport logs from Kafka to HDFS in [$host]----------"
        # 在对应主机上启动flume agent
        ssh $host "
        source /etc/profile
        cd $flume_home
        nohup ./bin/flume-ng agent -n $agent_name -f $agent_conf -c $flume_conf >/dev/null 2>&1 &
        "
    done
} ;;
"stop") {
    for host in $cluster; do
        echo -e "\n----------Stopping transporting logs from Kafka to HDFS in [$host]----------"
        # 在对应主机上关闭flume agent
        ssh $host 2>/dev/null <<EOF
        source /etc/profile
        PID=\$(ps -ef | grep kafka-hdfs | grep -v grep | awk '{print \$2}')
        if [ "\$PID" = "" ];then
            echo -e "\n----------No logs-collect process to kill!----------"
        else
            echo \$PID | xargs kill
        fi
EOF
    done
} ;;
*) {
    echo -e "\nWrong Parameters!"
    exit 1
} ;;
esac

# 获取脚本结束时间
end_time=$(date +%s)
# 计算脚本运行时间
execution_time=$(($end_time - $start_time))
# 打印运行时间
echo -e "\n----------$operation transport logs in [$cluster] takes ${execution_time} seconds----------\n"
