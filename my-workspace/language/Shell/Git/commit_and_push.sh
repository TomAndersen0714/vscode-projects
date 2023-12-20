#!/bin/bash
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
