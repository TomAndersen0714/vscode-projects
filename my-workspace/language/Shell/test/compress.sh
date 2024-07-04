#!/bin/bash

# 此脚本用于压缩指定文件,并计算压缩时间

# 获取当前时间(相对时间)
start_time=$(date +%s)

# 获取结束时间
end_time=$(date +%s)
# 计算运行时间
execution_time=$((${end_time} - ${start_time}))
echo -e "\n----------execute \"$*\" in cluster takes ${execution_time} seconds----------\n"


问题:
1. 为什么 spark.sql.shuffle.partitions 未生效
2. 为什么 Executor 在schedule的时候就挂了



