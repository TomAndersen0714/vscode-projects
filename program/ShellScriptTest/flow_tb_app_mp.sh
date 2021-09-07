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
param_yesterday_day=`date -d "$1 -2 day" "+%Y%m%d"`
param_three_day_ago=`date -d "$1 -4 day" "+%Y%m%d"`
echo $param_day $param_month $param_year $param_date $param_yesterday_date $param_two_days_ago_date $param_yesterday_day $param_three_day_ago



impala-shell -f $sql_path/dwd.question_b_wt.sql


# 各个平台消息统计
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/msg_day_platform.sql
# 店铺消息统计
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/msg_day_platform_nick.sql
# 店铺子账号消息统计
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/msg_day_platform_subnick.sql
# 同步报表
python3 $py_path/impala2mongo.py  -c $json_path/out/xd_stat_mon_taodongli_shop_stat.json --date=$param_day
python3 $py_path/impala2mongo.py  -c $json_path/out/xd_stat_mon_tb_shop_stat.json --date=$param_day
python3 $py_path/impala2mongo.py  -c $json_path/out/xd_stat_robot_stat.json --date=$param_day
python3 $py_path/impala2mongo.py  -c $json_path/out/xd_stat_mon_taodongli_subnick_stat.json --date=$param_day
python3 $py_path/impala2mongo.py  -c $json_path/out/xd_stat_mon_tb_subnick_stat.json --date=$param_day

# 店铺商品热度统计
impala-shell  --var="param_day=${param_day}" --var="param_month=${param_month}" --var="param_year=${param_year}" --var="param_date=${param_date}" -f $sql_path/xdmp_daily_request_goods_stat.sql
python3 $py_path/impala2mongo.py -c $json_path/out/tb_daily_request_goods_stat.json --date=${param_day}

# 店铺问题统计
# stage 1
impala-shell  --var="param_day=${param_day}" --var="param_month=${param_month}" --var="param_year=${param_year}" --var="param_date=${param_date}" -f $sql_path/inter_robot_shop_stat_by_question.sql
# stage 2
impala-shell  --var="param_day=${param_day}" --var="param_month=${param_month}" --var="param_year=${param_year}"  -f $sql_path/aggregate_robot_shop_stat_by_question.sql
python3 $py_path/impala2mongo.py -c $json_path/out/tb_robot_shop_stat_by_question.json --date=${param_day}
# 店铺问题趋势统计
impala-shell  --var="param_day=${param_day}" --var="param_month=${param_month}" --var="param_year=${param_year}" --var="param_date=${param_date}" -f $sql_path/xdmp_stat_question_for_shop.sql
# 6min.
python3.6 $py_path/impala2mongo.py -c $json_path/out/offline_tb_stat_question_for_shop.json --date=${param_day}   --update=False
python3.6 $py_path/impala2mongo.py -c $json_path/out/offline_pdd_stat_question_for_shop.json --date=${param_day}  --update=False

# 店铺-商品-问题 统计
impala-shell  --var="param_day=${param_day}" --var="param_month=${param_month}" --var="param_year=${param_year}" --var="param_date=${param_date}" -f $sql_path/xd_stat_stat_by_question_and_goods.sql
#修改为insert
python3 $py_path/impala2mongo.py -c $json_path/out/xd_stat_stat_by_question_and_goods.json --date=${param_day}  --update=False

#商品推荐统计
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/app_mp.goods_guide_required_stat.sql
python3.6 $py_path/impala2mongo.py -c $json_path/out/app_mp.goods_guide_required_stat.json --date=${param_day}

#店铺GMV
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/app_mp.tb_order.sql
#严格口径的应答率统计
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/app_mp.fix_tb_msg_day_platform_nick.sql
#商品咨询量统计
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/tdl_goods_question_stat.sql
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/tb_goods_question_stat.sql

#接待统计
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/reception_day_platform_nick.sql
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/reception_day_platform_snick.sql
python3 $py_path/impala2mongo.py -c $json_path/out/tb_reception_of_nick_stat_by_day.json

# 尺码表统计
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/size_day_platform_category_snick.sql
# 自学习统计
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" --var="param_yesterday_date=${param_yesterday_date}"  -f $sql_path/auto_learn.sql
python3 $py_path/impala2mongo.py  -c $json_path/out/log_mining.json --date=$param_day
# 明察统计
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/mc.sql
impala-shell  --var="param_day=${param_day}" -f $sql_path/category_qid_stat.sql

#响应时间统计
spark-submit --master yarn-client --driver-memory 3G --executor-memory 3G --num-executors 2 --executor-cores 1 --driver-cores 1 --conf spark.dynamicAllocation.enabled=false --verbose --class com.xiaoduo.responseStatisticByQuestion /data1/tmp/udfs/impalaUDF.jar tb
impala-shell -q "refresh xd_stat.response_stat_by_question;"
spark-submit --master yarn --deploy-mode client --driver-memory 3G --executor-memory 3G --num-executors 2 --executor-cores 1 --driver-cores 1 --conf spark.dynamicAllocation.enabled=false --verbose --class com.xiaoduo.responseStatisticByQuestion /data1/tmp/udfs/impalaUDF.jar taodongli
impala-shell -q "refresh xd_stat.response_stat_by_question_taodongli;"
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/tb_response_stat.sql
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/tb_response_stat_tdl.sql

#店铺、子账号接待
spark-submit --master yarn --deploy-mode client --driver-memory 6G --executor-memory 3G --num-executors 3 --executor-cores 2 --driver-cores 1 --conf spark.dynamicAllocation.enabled=false --verbose --class com.xiaoduo.receptionStatistic /data1/tmp/udfs/impalaUDF.jar ${param_day}

impala-shell -q "ALTER TABLE xd_stat.reception_stat recover PARTITIONS"

sleep 5
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/tb_sub_nick_receive.sql
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/tb_shop_receive.sql
sleep 5
python3.6 $py_path/impala2mongo.py  -c $json_path/out/tb_sub_nick_receive.json --date=${param_day} --update=True
python3.6 $py_path/impala2mongo.py  -c $json_path/out/tb_shop_receive.json --date=${param_day} --update=True


#售前数据统计
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/presale_day_platform_snick_goods_question.sql
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/presale_day_platform_snick_question.sql
impala-shell  --var="param_date=${param_date}" -f $sql_path/presale_day_platform_snick_goods.sql
impala-shell  --var="param_date=${param_yesterday_date}" -f $sql_path/presale_day_platform_snick_goods.sql
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/presale_day_platform_snick_question_msg_example.sql
impala-shell  --var="param_day=${param_day}" --var="param_date=${param_date}" --var="param_yesterday_day=${param_yesterday_day}" -f $sql_path/presale_day_platform_snick_goods_question_msg_example.sql


impala-shell -f sql_scripts/app_mp.question_b_summary.sql 
impala-shell -q "alter table xd_stat.presale_day_platform_snick_goods_question_msg_example_distinct_by_week drop partition (day=${param_three_day_ago})"

# 接待统计 针对西婷化妆品专营店店铺覆盖接待次数，新逻辑 act = 'send_msg' and send_msg_from = '2'
impala-shell   --var="param_day=${param_day}" --var="param_date=${param_date}" -f $sql_path/tb_sub_nick_receive_add_extention.sql
sleep 5
python3.6 $py_path/impala2mongo.py  -c $json_path/out/tb_sub_nick_receive_add_extention.json --date=${param_day} --update=True
