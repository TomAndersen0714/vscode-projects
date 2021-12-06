# 此脚本用于启动/停止Kafka集群
# 输入的参数只能为start/stop

# 判断参数个数
if (($# > 1)); then
    echo "Wrongs parameters!"
    exit 1
fi

# 获取当前时间(相对时间)
start_time=$(date +%s)
# 获取操作方式
operation=$1
# 设置Kafka集群
cluster=${KAFKA_CLUSTER:-"hadoop101 hadoop102 hadoop103"}

# 对kafka集群进行对应的操作
case "$operation" in
start)
    echo "----------Starting kafka cluster----------"
    for host in $cluster; do
        echo "----------Starting kafka in [$host]----------"
        ssh $host "source /etc/profile;
        KAFKA_HOME=\${KAFKA_HOME:-'/opt/module/kafka_2.11-2.1.1'};
        cd \$KAFKA_HOME;
        nohup ./bin/kafka-server-start.sh config/server.properties > /dev/null 2>&1 &
        "
    done
    ;;
stop)
    echo "----------Stopping kafka cluster----------"
    for host in $cluster; do
        echo "----------Starting kafka in [$host]----------"
        ssh $host "source /etc/profile;
        KAFKA_HOME=\${KAFKA_HOME:-'/opt/module/kafka_2.11-2.1.1'};
        cd \$KAFKA_HOME;
        ./bin/kafka-server-stop.sh
        "
    done
    ;;
*)
    echo "Worong Parameter!"
    exit 1
    ;;
esac

# 获取结束时间
end_time=$(date +%s)
execution_time=$((${end_time} - ${start_time}))
echo -e "\n----------$operation kafka in [$cluster] takes ${execution_time} seconds----------\n"
