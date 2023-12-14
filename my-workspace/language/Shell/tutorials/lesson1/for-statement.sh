#!/bin/bash


# 遍历 1 到 10
for i in {1..10}; do
    # 如果 i 等于 5，跳出循环
    if [ $i -eq 5 ]; then
        break
    fi
    # 打印 i
    echo $i
done