#!/bin/bash
# 此脚本用于每天凌晨执行
# 当未传入参数时,默认统计前一天的各个周期下的活跃用户,并填充 ads_uv_count 表
# 当传入单个参数时,统计指定日期的各个周期下的活跃用户,并填充 ads_uv_count 表
# 本脚本最多只支持传入单个参数,且必须为特定格式日期,即:yyyy-MM-dd,如:2020-06-22

# 判断参数个数,只支持不传入参数,或者只传入单个参数
if (($# > 1)); then
    echo "This shellscript takes only one parameter at most!"
    exit 1
fi
# 获取当前时间(相对时间)
start_time=$(date +%s)
# 日志收集日期
log_date=""
# hive client主机
hive_client="hadoop101"
# hive_bin_dir
hive_bin_dir=${HIVE_HOME:-"/opt/module/hive-2.3.0"}/bin
# 登录hive客户端主机所用用户名
user="tomandersen"
# hive仓库名
warehouse="gmall"

# 如果输入参数为空,则将其设置为昨天的日期
# 如果输出参数不为空,则判断输入的参数是否符合日期格式,并尝试将其转换成标准日期格式
if [ "$1" = "" ]; then
    log_date=$(date -d '-1 day' +%F)
else
    log_date=$(date -d $1 +%F) || exit 1
fi;

# 用于执行的hive-sql语句(不支持注释)
hive_sql="
set hive.execution.engine=tez;

set hive.strict.checks.cartesian.product=false;

CREATE TABLE IF NOT EXISTS ${warehouse}.ads_uv_count(
    dt string COMMENT '统计日期',
    day_count bigint COMMENT '当天活跃用户数',
    wk_count bigint COMMENT '当周活跃用户数',
    mn_count bigint COMMENT '当月活跃用户数',
    is_weekend string COMMENT '(Y/N)是否是周末,用于统计整周数据',
    is_monthend string COMMENT '(Y/N)是否是月末,用于统计整月数据'
) COMMENT '活跃用户统计表'
ROW FORMAT
DELIMITED FIELDS TERMINATED BY '\t';

INSERT INTO TABLE ${warehouse}.ads_uv_count
SELECT
    '${log_date}' AS dt,
    tbl_1.count AS day_count,
    tbl_2.count AS wk_count,
    tbl_3.count AS mn_count,
    if(date_add(next_day('${log_date}','MONDAY'),-1)='${log_date}','Y','N') AS is_weekend,
    if(last_day('${log_date}')='${log_date}','Y','N') AS is_monthend
FROM
(
    SELECT count(1) AS count
    FROM ${warehouse}.dws_uv_detail_day
    WHERE dt='${log_date}'
)AS tbl_1,
(
    SELECT count(1) AS count
    FROM ${warehouse}.dws_uv_detail_wk
    WHERE wk_dt=concat(
        date_add(next_day('${log_date}','MONDAY'),-7),'-',
        date_add(next_day('${log_date}','MONDAY'),-1))
)AS tbl_2,
(
    SELECT count(1) AS count
    FROM ${warehouse}.dws_uv_detail_mn
    WHERE mn=date_format('${log_date}','yyyy-MM')
)AS tbl_3;
"

# 登录hive client执行hive sql语句
ssh -T $user@$hive_client <<EOF
    $hive_bin_dir/hive -e "$hive_sql"
EOF

# 获取结束时间
end_time=$(date +%s)
# 计算执行时间
execute_time=$(($end_time - $start_time))
echo -e "\n----------Transport logs from dws to ads hive table on [$host] takes $execute_time seconds----------\n"