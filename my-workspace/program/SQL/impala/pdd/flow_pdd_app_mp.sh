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
#店铺统计
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/app_mp.msg_day_platform_nick.sql
python3.6 $py_path/impala2mongo.py -c $json_path/out/xd_stat_mon_pdd_shop_stat.json --date=${param_day}
#子账号统计
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/app_mp.msg_day_platform_subnick.sql
python3.6 $py_path/impala2mongo.py -c $json_path/out/xd_stat_mon_pdd_subnick_stat.json --date=${param_day}


#问题趋势统计
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/app_mp.pdd_stat_question_for_shop.sql
#impala-shell -q "invalidate metadata"
#python3.6 $py_path/impala2mongo.py -c $json_path/out/pdd_stat_question_for_shop.json --date=${param_day}
python3.6 $py_path/impala2mongo.py -c $json_path/out/offline_pdd_stat_question_for_shop.json --date=${param_day}  --update=False

#导出商品TOP
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/app_mp.pdd_daily_request_goods_stat.sql
python3.6 $py_path/impala2mongo.py -c $json_path/out/pdd_daily_request_goods_stat.json  --date=${param_day}
#导出问题应答情况
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/app_mp.pdd_robot_shop_stat_by_question.sql
python3.6 $py_path/impala2mongo.py -c $json_path/out/pdd_robot_shop_stat_by_question.json  --date=${param_day}

#接待金额统计
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/app_mp.pdd_order.sql

impala-shell --var="param_date=${param_date}" -f $sql_path/app_mp.ppd_reception_of_nick_stat_by_day_2.sql

impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/app_mp.pdd_reception_of_nick_stat_by_day.sql

python3.6 $py_path/impala2mongo.py -c $json_path/out/pdd_reception_of_nick_stat_by_day.json --date=${param_day}



#催单统计日任务
impala-shell --var="param_day=${param_day}"  --var="param_date=${param_date}" -f $sql_path/app_mp.pdd_reminder_day_platform_nick.sql
impala-shell --var="param_day=${param_day}"  --var="param_date=${param_date}" -f $sql_path/app_mp.pdd_reminder_day_platform_subnick.sql
impala-shell  --var="param_start_day=${param_yesterday_day}"  --var="param_end_day=${param_day}"   --var="param_date=${param_yesterday_date}" -f $sql_path/app_mp.pdd_reminder_day_platform_nick_backfill.sql

#python3.6 $py_path/impala2mongo.py -c $json_path/out/app_mp.pdd_reminder.json --date=${param_day}
#python3.6 $py_path/impala2mongo.py -c $json_path/out/app_mp.pdd_reminder.json --date=${param_yesterday_day}

impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/app_mp.pdd_order.sql




