#!/bin/bash


# 遍历 1 到 10
for i in {1..10}; do
    # 如果 i 等于 5，跳出循环
    if [ $i -eq 5 ]; then
        break
    fi
    # 打印 i
    echo "$i"
done


# 遍历 Array
paths=(
    '/root/workspace/project/vscode-projects'
    '/root/workspace/project/notebooks'
)

for path in "${paths[@]}"; do
    date
    cd "$path" || exit
    echo "$path"
    git add \*
    git commit -m "update $(date)"
    git push origin master
    printf "%0.s-" {1..40} && echo
    echo
done