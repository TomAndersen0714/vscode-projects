#!/bin/bash

cd /home/worker/xiaoduo_bigdata/app_mp

sql_path=/home/worker/xiaoduo_bigdata/app_mp/fix_sql_scripts
json_path=/home/worker/xiaoduo_bigdata/app_mp/json_scripts
py_path=/home/worker/xiaoduo_bigdata/data_sync/python_scripts
#51 119
for i in $(seq 51 119)  
do   
    param_date=`date -d "$1 -$i day" "+%Y-%m-%d"`
    param_day=`date -d "$1 -$i day" "+%Y%m%d"`
    echo $param_date $param_day     
#    impala-shell  --var="param_date=${param_date}" -f $sql_path/presale_day_platform_snick_goods.sql
    impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/app_mp_pdd_goods_question_stat.sql
done 
#impala-shell  --var="param_date=${param_date}" -f $sql_path/presale_day_platform_snick_goods.sql
#impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/tb_goods_question_stat.sql
