#!/bin/bash
# 此脚本用于执行一些同时在集群中执行的命令,如jps/date等
# 使用的用户与当前登录用户相同

# 获取当前时间(相对时间)
start_time=$(date +%s)
# 集群设置
cluster="hadoop103 hadoop102 hadoop101"

# 当前登录用户
user=$(whoami)
# 当前路径
dir=$(pwd)

for host in $cluster; do
    echo "----------$host----------"
    ssh -T $user@$host <<EOF
    cd $dir && $*
EOF
done

# 获取结束时间
end_time=$(date +%s)
# 计算运行时间
execution_time=$((${end_time} - ${start_time}))
echo -e "\n----------execute \"$*\" in cluster takes ${execution_time} seconds----------\n"
