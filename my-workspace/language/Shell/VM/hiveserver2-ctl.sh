#!/bin/bash
# 此脚本用于控制Hiveserver2服务进程

# 判断参数个数
if (($# != 1)); then
    echo "Only one parameter is needed(start/stop)!"
    exit 1
fi

# 获取当前时间(相对时间)
start_time=$(date +%s)
# 获取操作选项
operation=$1
# hiveserver2服务主机
host=hadoop101
# 远程操作用户
user=tomandersen

case "$operation" in
"start")
    echo -e "\n----------Starting the HiveServer2 service on [$host]----------"
    ssh -T $user@$host <<EOF
        # 获取HIVE_BIN_DIR路径
        HIVE_BIN_DIR="\${HIVE_HOME:-/opt/module/hive-2.3.0}/bin"
        # 启动hiveserver2服务
        nohup \$HIVE_BIN_DIR/hiveserver2 1>/tmp/hive/logs/hiveserver2.log 2>&1 &
EOF
    ;;
"stop")
    echo -e "\n----------Stopping the HiveServer2 service on [$host]----------"
    ssh -T $user@$host <<EOF
        PID=$(ps -ef | grep -v grep | grep HiveServer2 | grep -v $0 | awk '{print $2}' | xargs)
        if [ "\$PID" = "" ];then
            echo -e "\n----------There is no HiveServer2 service thread on [$host]----------"
        else
            echo \$PID | xargs kill
        fi;
EOF
    ;;
*)
    echo -e "\n----------Wrong parameter!----------"
    exit 1
    ;;
esac

# 获取结束时间
end_time=$(date +%s)
# 计算执行时间
execute_time=$(($end_time - $start_time))
echo -e "\n----------${operation}ed HiveServer2 service on [$host] takes $execute_time seconds----------\n"
