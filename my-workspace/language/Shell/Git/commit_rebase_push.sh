#!/bin/bash
repository_abs_paths=(
    '/root/workspace/project/vscode-projects'
    '/root/workspace/project/notebooks'
)

for path in "${repository_abs_paths[@]}"; do
    date
    cd "$path" || exit
    echo "$path"
    git fetch
    git add .
    git commit -m "update $(date)"
    git rebase origin_gitee/master
    git push
    printf "%0.s-" {1..40} && echo
    echo
done
