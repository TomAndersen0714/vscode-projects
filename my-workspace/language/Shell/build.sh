#!/bin/bash
# 此脚本用于提交 git commit, 以及打包完整项目并同步

set -ex

# get parameters
git_path=${1:-"."}
option=${2:-"commit"}
trailing_params=$3


# check parameters
if [ ! -d "$git_path" ]; then
    echo "git path not exist"
    exit 1
fi

if [ "$option" != "commit" ] && [ "$option" != "pack" ] && [ "$option" != "push" ]; then
    echo "option must be commit, pack or push"
    exit 1
fi

# handle parameters
git_repo=$(basename "$(git -C "${git_path}" rev-parse --show-toplevel)")

# define functions
function git_commit() {
    # handle commit message
    if [ -z "$trailing_params" ]; then
        trailing_params="update $(date)"
    else
        trailing_params="update $(date) $trailing_params"
    fi

    # git commit
    git -C "$git_path" add .
    git -C "$git_path" commit -m "$trailing_params"
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

    # remove expired git_repo package
    rm -f "$pkg_file"
    find "$sync_path_1" -name "${git_repo}_*.zip" -type f -mtime +2 -exec rm -f '{}' ';'
    find "$sync_path_2" -name "${git_repo}_*.zip" -type f -mtime +2 -exec rm -f '{}' ';'
}

function git_push() {
    # git push
    if [ -z "$trailing_params" ]; then
        git -C "$git_path" push
    else
        git -C "$git_path" push "$trailing_params"
    fi
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
push)
    git_push
    ;;
esac
