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
# 根据操作模式参数启动/关闭FLume Agent
case "$operation" in
"start") {
    for host in $cluster; do
        echo -e "\n----------Starting to transport logs from Kafka to HDFS in [$host]----------"
        # 在对应主机上启动flume agent
        ssh $host <<EOF
        source /etc/profile
        flume_home=\${FLUME_HOME:-"/opt/module/flume-1.8.0/"}
        cd $flume_home
        nohup ./bin/flume-ng agent -n a1 -c conf \
        -f job/kafka-hdfs.properties >/dev/null 2>&1 &
EOF
    done
} ;;
"stop") {
    for host in $cluster; do
        echo -e "\n----------Stopping transporting logs from Kafka to HDFS in [$host]----------"
        # 在对应主机上关闭flume agent
        ssh $host 2>/null/dev <<EOF
        source /etc/profile
        PID=\$(ps -ef | gerp kafka-hdfs | grep -v grep | awk '{print \$2}')
        if [ -z "\$PID" ];then
            echo -e "\n----------No logs-collect process to kill!----------"
        else
            echo "\$PID" | xargs kills
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
end_time=$(date +%s);
# 计算脚本运行时间
execution_time=$[ $end_time-$start_time ]
# 打印运行时间
echo -e "\n----------$operation transport logs in [$cluster] takes ${execution_time} seconds----------\n"