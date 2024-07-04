#!/bin/bash
# 功能: 在指定文件夹下检索指定文本, 检索文本支持 include 和 exclude pattern, 命中后输出 pattern 和命中文件的路径

set -ex

# input parameters
search_directory=$1

# get timestamp and output file
timestamp=$(date +%s)
output_file="output/output_${timestamp}.txt"

# include and exclude pattern
include_pattern="submit"
exclude_pattern="hive"


# find every file full path in the input file
find "${search_directory}" -type f -exec sh -c 'grep -qE "$1" "$0" && ! grep -qE "$2" "$0" && echo "include_pattern: $1, exclude_pattern: $2, file_path: $0"' {} "${include_pattern}" "${exclude_pattern}" \; >> "${output_file}"


# find tmp/ -type f -exec sh -c 'grep -qE "submit" "$0" && ! grep -qE "hive" "$0" && echo "$0"' {} \;
# find tmp/ -type f -exec sh -c 'grep -qE "$1" "$0" && ! grep -qE "$2" "$0" && echo "$0"' {} "${include_pattern}" "${exclude_pattern}" \;