#!/bin/bash
# 此脚本用于在指定主机上生成测试logs
# 单机启动命令为 java -cp /opt/module/log-collector-1.0-SNAPSHOT-jar-with-dependencies.jar com.atguigu.appclient.AppMain $1 $2 > /dev/null
# 参数 arg_output 为输出路径,在logback.xml中已经配置了默认值/tmp/logs
# 参数 arg_interval 为日志生成时间间隔
# 参数 arg_count 为生成的日志记录个数
# 本脚本支持的选项为 -i 时间间隔,-o 日志输出路径,-c 为生成的日志记录个数
echo
# 定义 usage 函数,应用于 -h 选项
usage() {
    echo "
    usage: [-i <interval>] [-o <output dir>] [-c <count of records>] [-h]
    -i 指定日志记录生成时间间隔,单位为毫秒ms,默认为0
    -o 指定日志文件输出路径,默认为 /tmp/logs
    -c 指定日志记录生成总数,默认为 100
    -h 输出此页面
    "
}

# 使用getopt解析选项,并使用set命令将当前脚本参数转换成格式化后的参数
args=$(getopt i:o:c:h $@)
if (($? != 0)); then
    exit 1
else
    set -- $args
fi
# 定义参数默认值
# 默认日志输出路径为/tmp/logs
arg_output="/tmp/logs"
# 默认值时间间隔为0
arg_interval=0
# 默认日志记录生成总数为100
arg_count=100

# 获取格式化后的参数
while [ "$1" != "" ]; do
    case "$1" in
    "-i")
        arg_interval=$2
        shift
        ;;
    "-o")
        arg_output=$2
        shift
        ;;
    "-c")
        arg_count=$2
        shift
        ;;
    "-h")
        usage
        exit 0
        ;;
    "--")
        shift
        break
        ;;
    *)
        echo "Unknown option $1"
        usage
        exit 1
        ;;
    esac
    shift
done

# 输出各个参数对应值
# 日志输出路径,默认为/tmp/logs
echo "output dir: ${arg_output}"
# 日志记录生成时间间隔,默认为0
echo "interval: ${arg_interval} ms"
# 日志记录生成总数,默认为100
echo "count: ${arg_count}"

# 获取当前时间(相对时间)
start_time=$(date +%s)
# 设置负责生成日志的集群
cluster="hadoop101 hadoop102"

# 日志生成工具jar包路径
jar_classpath="/opt/module/log-generator-1.0-SNAPSHOT-jar-with-dependencies.jar"
# 日志生成类全类名
app_main_class="com.atguigu.appclient.AppMain"

# 遍历集群生成日志
for host in $cluster; do
    echo -e "\n----------Creating logs in $host:${arg_output}----------"
    ssh $host "source /etc/profile;java -Dlog_home=${arg_output} \
    -cp  ${jar_classpath} ${app_main_class} ${arg_interval} ${arg_count} > /dev/null"
done

# 获取结束时间
end_time=$(date +%s)
execution_time=$((${end_time} - ${start_time}))
echo -e "\n----------Created test logs in [$cluster] takes ${execution_time} seconds----------\n"
