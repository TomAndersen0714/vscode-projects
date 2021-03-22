#!/bin/bash
# 此脚本用于在指定主机上生成测试logs
# 单机启动命令为 java -cp /opt/module/log-collector-1.0-SNAPSHOT-jar-with-dependencies.jar com.atguigu.appclient.AppMain $1 $2 > /dev/null
# 参数1为输出路径,在logback.xml中已经配置了默认值/tmp/logs
# 参数2为日志生成时间间隔,参数3为日志生成次数
# 其中参数1输出路径必须要传参,可以是空字符串(即使用路径)

# 判断控制参数个数
if (($# > 3)); then
    echo "Wrongs parameters!" >&2
    exit 1
fi

# 获取当前时间(相对时间)
start_time=$(date +%s)
# 负责生成日志的集群
cluster="hadoop101 hadoop102"

# 设置日志输出路径,默认为/tmp/logs
log_dir=${1:-"/tmp/logs"}
# 设置日志生成时间间隔,默认为0
interval=${2:-0}
# 设置日志生成条数,默认为1000条
count=${3:-1000}


# 遍历集群生成日志
for host in $cluster; do
    echo -e "\n----------Creating logs in $host:$log_dir----------"
    ssh $host "source /etc/profile;java -Dlog_home=$log_dir \
    -cp /opt/module/log-collector-1.0-SNAPSHOT-jar-with-dependencies.jar \
    com.atguigu.appclient.AppMain $interval $count > /dev/null"
done

# 获取结束时间
end_time=$(date +%s)
execution_time=$((${end_time} - ${start_time}))
echo -e "\n----------Created test logs in [$cluster] takes ${execution_time} seconds----------\n"