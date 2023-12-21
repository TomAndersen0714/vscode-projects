#!/bin/bash
# 此脚本用于修改集群时间

if (($# != 1)); then
    echo "Wrong Parameters!"
    exit 1
fi

# 获取当前时间(相对时间)
start_time=$(date +%s)
# 获取设定时间
setting_time = $1
# 集群设置
cluster=${CLUSTER:-"hadoop101 hadoop102 hadoop103"}

for host in $cluster; do
    echo -e "\n----------Setting the date in $host----------"
    ssh $host "source /etc/profile;sudo date -s $setting_time"
done

end_time=$(date +%s)
execution_time=$((${end_time} - ${start_time}))
echo -e "\n----------Changed the cluster date to $setting_time takes ${execution_time} seconds----------\n"
