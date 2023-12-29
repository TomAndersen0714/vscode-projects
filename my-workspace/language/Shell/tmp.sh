#!/bin/bash

if [ ! $# -eq 2 ]; then
    echo "need two date as parameters"
    exit -9
fi

batch_date=$(date -d ${1} +%Y-%m-%d 2>/dev/null)
# 生命周期天数
ttl_day=${2}

backup_date=$(date -d "$batch_date - $ttl_day days" "+%Y-%m-%d")

echo "backup_date: $backup_date"

echo "{"'"batch_date"':'"'$batch_date'"','"backup_date"':'"'$backup_date'"', '"ttl_day"':'"'$ttl_day'"'"}" 