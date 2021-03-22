#!/bin/bash
# 此脚本用于启动flume agent实现日志采集,并将采集到的日志数据按照类别生产到
# Kafka不同的Topic中去
# 参数只能为start或stop

# 判断参数个数
if (($# != 1)); then
    echo "Wrongs parameter!"
    exit 1
fi

# 获取当前时间(相对时间)
start_time=$(date +%s)
# 获取操作方式
operation=$1
# 设置用于采集日志数据的主机集群
cluster_for_collect_logs="hadoop101 hadoop102"
# 设置flume_home目录
flume_home=${FLUME_HOME:-"/opt/module/flume-1.8.0"}
# 设置flume配置文件目录
flume_conf="conf"
# 设置flume agent配置文件目录
agent_conf="job/taildir-kafka.properties"
# 设置flume agent名称(要与agent_conf对应的配置文件中的agent名称相对应)
agent_name="a1"

case "$operation" in
"start")
    for host in $cluster_for_collect_logs; do
        echo -e "\n----------Starting flume agent(collect logs) in [$host]----------"
        ssh $host "
        source /etc/profile;
        cd $flume_home;
        nohup ./bin/flume-ng agent -n $agent_name -c $flume_conf -f $agent_conf >/dev/null 2>&1 &
        "
    done
    ;;
"stop")
    for host in $cluster_for_collect_logs; do
        echo -e "\n----------Stopping collecting logs in [$host]----------"
        # ssh远程执行命令时启动的美元符号$需要加转义字符,否则是读取当前终端变量
        # 若远程执行的命令中带有双引号",则不能使用双引号限定命令,而应该使用首尾定界符
        # 首尾定界符名称随意,只需要配对上
        ssh $host -T <<EOF
        source /etc/profile;
        PID=\$(ps -ef | grep -v grep | grep taildir-kafka -n | awk '{print \$2}' | xargs);
        if [ "\$PID" = "" ];then
            echo -e "\n----------No logs collect process to kill in [$host]----------"
        else
            echo "\$PID" | xargs kill
        fi;
EOF
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
echo -e "\n----------$operation collecting logs in [$cluster_for_collect_logs] takes ${execution_time} seconds----------\n"
