#!/bin/bash

cd /home/worker/xiaoduo_bigdata/app_mp

sql_path=/home/worker/xiaoduo_bigdata/app_mp/sql_scripts
json_path=/home/worker/xiaoduo_bigdata/app_mp/json_scripts
py_path=/home/worker/xiaoduo_bigdata/data_sync/python_scripts

param_date=`date -d yesterday "+%F"`
param_yesterday_date=`date -d "$1 -2 day" "+%Y-%m-%d"`
param_two_days_ago_date=`date -d "$1 -3 day" "+%Y-%m-%d"`
param_day=`date -d yesterday "+%Y%m%d"`
param_month=`date -d yesterday "+%Y%m"`
param_year=`date -d yesterday "+%Y"`
echo $param_day $param_month $param_year $param_date $param_yesterday_date $param_two_days_ago_date


begin_date="20200901"
end_date="20200911"

while [ "$begin_date" -le "$end_date" ];
do
    param_date=$(date -d "${begin_date}" +%F)
    param_day=$(date -d "${begin_date}" +%Y%m%d)
    echo $param_date, $param_day
    #导出商品TOP
    impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/app_mp.pdd_daily_request_goods_stat.sql
    python3.6 $py_path/impala2mongo.py -c $json_path/out/pdd_daily_request_goods_stat.json  --date=${param_day}
    begin_date=$(date -d "${begin_date}+1days" +%Y%m%d)
done