#!/bin/bash
param_day=`date -d yesterday "+%Y%m%d"`
py_path=/home/worker/xiaoduo_bigdata/data_sync/python_scripts
json_path=/home/worker/xiaoduo_bigdata/data_sync/json_scripts/in
sql_path=/home/worker/xiaoduo_bigdata/data_sync/sql_scripts

python3.6 $py_path/mongo2impala.py -c $json_path/pdd_xdmp_category.json --update=True
python3.6 $py_path/mongo2impala.py -c $json_path/pdd_xdmp_pdd_shop_sub.json --update=True
python3.6 $py_path/mongo2impala.py -c $json_path/pdd_xdmp_shop.json --update=True
python3.6 $py_path/mongo2impala.py -c $json_path/pdd_xdmp_user_service.json --update=True
#python3.6 $py_path/mongo2impala.py -c $json_path/pdd_xdmp_goods.json --update=True
echo 1
python3.6 $py_path/mongo2impala.py -c $json_path/pdd_xdmp_reminder.json --update=True
impala-shell -f $sql_path/pdd_xdmp_reminder.sql
echo 2
#python3.6 $py_path/mongo2impala.py -c $json_path/pdd_xdmp_order.json --update=True --time=${param_day} --mode=daily --timeZoneFix=True

#python3.6 $py_path/mongo2impala.py -c $json_path/pdd_xdrs_logs.json --update=True --time=${param_day} --mode=daily
