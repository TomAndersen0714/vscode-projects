#!/bin/bash
# 使用rsync将文件或者文件夹同步到集群所有主机
# 1. 获取输入参数个数，如果没有参数则直接退出
pcount=$#
if ((pcount == 0)); then
    echo "No parameters!"
    exit 1
fi

# 2. 若有输入参数则获取文件名
# 获取第一个参数
p1=$1
# 获取文件名或文件夹名
fName=$(basename $p1)
echo fName=$fName
# 3. 获取上级目录到绝对路径
# 以物理路径进入，避免使用了因使用了路径链接而存放到错误路径下
# 获取父路径
parentDir=$(
    cd -P $(dirname $p1)
    pwd
)
echo parentDir=$parentDir

# 4. 获取当前用户名称
user=$(whoami)

# 5. 遍历主机hadoop102~hadoop103，将制定路径传输
for ((host = 101; host <= 103; host++)); do
    echo "---------hadoop$host----------"
    rsync -rvl $parentDir/$fName $user@hadoop$host:$parentDir
done
# 另一种遍历方法，适用于小集群
# 设置集群(仅适合于集群数量较小时,当集群数量较大时还是要遍历序号)
# cluster="hadoop101 hadoop102 hadoop103"
# 遍历集群,分发文件
# for host in $cluster; do
#     echo "---------$host---------"
#     rsync -rvl $parentDir/$fName $user@$host:$parentDir
# done
