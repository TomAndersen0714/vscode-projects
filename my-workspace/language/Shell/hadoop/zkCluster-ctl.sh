#!/bin/bash
# 此脚本用于控制zookeeper集群，使用方式和zkServer.sh相同，前提是设置了ZOOKEEPER_HOME环境变量

if (($# != 1)); then
    echo "Wrong parameters!"
    exit 1
fi
# 获取当前时间(相对时间)
start_time=$(date +%s)
# 获取当前用户
user=$(whoami)
# 集群的ip地址列表
cluster=${ZKCluster:-"zkServer1 zkServer2 zkServer3"}

# 根据输入参数调用对应功能
case $1 in
"start")
    echo -e "\n----------Starting zookeeper cluster----------"
    for host in $cluster; do
        echo -e "\n----------Starting zookeeper in [${host}]----------"
        ssh $user@$host "source /etc/profile;$ZOOKEEPER_HOME/bin/zkServer.sh start"
    done
    ;;
"status")
    echo -e "\n----------Getting zookeeper status----------"
    for host in $cluster; do
        echo -e "\n----------Getting status in [${host}]----------"
        ssh $user@$host "source /etc/profile;$ZOOKEEPER_HOME/bin/zkServer.sh status"
    done
    ;;
"stop")
    echo -e "\n----------Stopping zookeeper cluster----------"
    for host in $cluster; do
        echo -e "\n----------Stopping zookeeper in [${host}]----------"
        ssh $user@$host "source /etc/profile;$ZOOKEEPER_HOME/bin/zkServer.sh stop"
    done
    ;;
*)
    echo "Worong parameter!"
    exit 1
    ;;
esac

end_time=$(date +%s)
execution_time=$((${end_time} - ${start_time}))
echo -e "\n----------$1 Zookeeper cluster takes ${execution_time} seconds----------\n"
