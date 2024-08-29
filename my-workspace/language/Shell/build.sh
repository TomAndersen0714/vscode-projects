#!/bin/bash
# 此脚本用于提交 git commit, 以及打包完整项目并同步

set -ex

# get parameters
git_path=${1:-"."}
option=${2:-"commit"}
git_commit_msg=$3


# check parameters
if [ ! -d "$git_path" ]; then
    echo "git path not exist"
    exit 1
fi

if [ "$option" != "commit" ] && [ "$option" != "pack" ]; then
    echo "option must be commit or pack"
    exit 1
fi

# handle parameters
git_repo=$(basename "$(git -C "${git_path}" rev-parse --show-toplevel)")
if [ -z "$git_commit_msg" ]; then
    git_commit_msg="update $(date)"
else
    git_commit_msg="update $(date) $git_commit_msg"
fi

# define functions
function git_commit() {
    # git commit
    git -C "$git_path" add .
    git -C "$git_path" commit -m "$git_commit_msg"
}

function zip_package() {
    # define variables
    pkg_file=${git_repo}_$(date +%Y%m%d%H%M%S).zip
    sync_path_1="./output/"
    sync_path_2="$HOME/onedrive/Packages/"

    # zip packing and overwrite old package
    zip -r "$pkg_file" "$git_path"
    cp "$pkg_file" "$sync_path_1"
    cp "$pkg_file" "$sync_path_2"

    # remove unused package
    rm -f "$pkg_file"
    # remove expired git_repo package
    find "$sync_path_1" -name "${git_repo}_*.zip" -type f -mtime +3 -exec rm -f '{}' ';'
    find "$sync_path_2" -name "${git_repo}_*.zip" -type f -mtime +3 -exec rm -f '{}' ';'
}

# main
# choose function to execute depend on option using case statement
case $option in
commit)
    git_commit
    ;;
pack)
    zip_package
    ;;
esac
