#!/bin/bash
begin_date='20200824'
end_date='20200914'

sql_path=/home/worker/xiaoduo_bigdata/app_mp/fix_sql_scripts
json_path=/home/worker/xiaoduo_bigdata/app_mp/json_scripts
py_path=/home/worker/xiaoduo_bigdata/data_sync/python_scripts

while (($begin_date < $end_date)); do
    echo $begin_date
    param_date=$(date -d "$begin_date" "+%Y-%m-%d")
    param_day=$(date -d "$begin_date" "+%Y%m%d")
    echo "$param_date, $param_day"
    # 修复pdd商品问题统计
    impala-shell --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/fix.app_mp.pdd_goods_question_stat.sql

    begin_date=$(date -d "${begin_date}+1days" +%Y%m%d)
done
