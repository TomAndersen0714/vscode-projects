#!/bin/bash
sql_path=/home/worker/xiaoduo_bigdata/app_mp/sql_scripts

param_date=`date -d yesterday "+%F"`
param_day=`date -d yesterday "+%Y%m%d"`
param_month=`date -d yesterday "+%Y%m"`
param_year=`date -d yesterday "+%Y"`

# 店铺-商品-问题 统计
impala-shell  --var="param_day=${param_day}" --var="param_month=${param_month}" --var="param_year=${param_year}" --var="param_date=${param_date}" -f $sql_path/app_mp_day_shop_question.sql