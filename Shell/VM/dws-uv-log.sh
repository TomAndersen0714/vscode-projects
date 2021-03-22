#!/bin/bash
# 此脚本用于每天凌晨执行
# 当未传入参数时,默认将前一天 dwd_start_log 中指定日期的日志分离到hive活跃用户表中
# 当传入单个参数时,获取 dwd_start_log 中指定日期的日志分离到hive活跃用户表中
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

set hive.exec.dynamic.partition.mode=nonstrict;

CREATE EXTERNAL TABLE IF NOT EXISTS ${warehouse}.dws_uv_detail_day(
    mid_id string COMMENT '设备唯一标识',
    user_id string COMMENT '用户标识',
    version_code string COMMENT '程序版本号',
    version_name string COMMENT '程序版本名',
    lang string COMMENT '系统语言',
    source string COMMENT '渠道号',
    os string COMMENT '手机操作系统',
    area string COMMENT '区域',
    model string COMMENT '手机型号',
    brand string COMMENT '手机品牌',
    sdk_version string COMMENT '开发包版本',
    gmail string COMMENT '邮箱',
    height_width string COMMENT '屏幕高宽',
    app_time string COMMENT '客户端日志产生的时间',
    network string COMMENT '网络模式',
    lng string COMMENT '经度',
    lat string COMMENT '纬度'
) COMMENT '日活用户分析表'
PARTITIONED BY(dt string);

INSERT OVERWRITE TABLE ${warehouse}.dws_uv_detail_day
PARTITION(dt='${log_date}')
SELECT
    mid_id,
    concat_ws(',',collect_set(user_id)) AS user_id,
    concat_ws(',',collect_set(version_code)) AS version_code,
    concat_ws(',',collect_set(version_name)) AS version_name,
    concat_ws(',',collect_set(lang)) AS lang,
    concat_ws(',',collect_set(source)) AS source,
    concat_ws(',',collect_set(os)) AS os,
    concat_ws(',',collect_set(area)) AS area,
    concat_ws(',',collect_set(model)) AS model,
    concat_ws(',',collect_set(brand)) AS brand,
    concat_ws(',',collect_set(sdk_version)) AS sdk_version,
    concat_ws(',',collect_set(gmail)) AS gmail,
    concat_ws(',',collect_set(height_width)) AS height_width,
    concat_ws(',',collect_set(app_time)) AS app_time,
    concat_ws(',',collect_set(network)) AS network,
    concat_ws(',',collect_set(lng)) AS lng,
    concat_ws(',',collect_set(lat)) AS lat
FROM ${warehouse}.dwd_start_log
WHERE dt='${log_date}'
GROUP BY mid_id;

CREATE TABLE IF NOT EXISTS ${warehouse}.dws_uv_detail_wk(
    mid_id string COMMENT '设备唯一标识',
    user_id string COMMENT '用户标识',
    version_code string COMMENT '程序版本号',
    version_name string COMMENT '程序版本名',
    lang string COMMENT '系统语言',
    source string COMMENT '渠道号',
    os string COMMENT '手机操作系统',
    area string COMMENT '区域',
    model string COMMENT '手机型号',
    brand string COMMENT '手机品牌',
    sdk_version string COMMENT '开发包版本',
    gmail string COMMENT '邮箱',
    height_width string COMMENT '屏幕高宽',
    app_time string COMMENT '客户端日志产生的时间',
    network string COMMENT '网络模式',
    lng string COMMENT '经度',
    lat string COMMENT '纬度',
    monday_date string COMMENT '周一日期',
    sunday_date string COMMENT '周日日期'
) COMMENT '周活用户明细表'
PARTITIONED BY(wk_dt string);

INSERT OVERWRITE TABLE ${warehouse}.dws_uv_detail_wk
PARTITION(wk_dt) 
SELECT
    mid_id,
    concat_ws(',',collect_set(user_id)) AS user_id,
    concat_ws(',',collect_set(version_code)) AS version_code,
    concat_ws(',',collect_set(version_name)) AS version_name,
    concat_ws(',',collect_set(lang)) AS lang,
    concat_ws(',',collect_set(source)) AS source,
    concat_ws(',',collect_set(os)) AS os,
    concat_ws(',',collect_set(area)) AS area,
    concat_ws(',',collect_set(model)) AS model,
    concat_ws(',',collect_set(brand)) AS brand,
    concat_ws(',',collect_set(sdk_version)) AS sdk_version,
    concat_ws(',',collect_set(gmail)) AS gmail,
    concat_ws(',',collect_set(height_width)) AS height_width,
    concat_ws(',',collect_set(app_time)) AS app_time,
    concat_ws(',',collect_set(network)) AS network,
    concat_ws(',',collect_set(lng)) AS lng,
    concat_ws(',',collect_set(lat)) AS lat,
    date_add(next_day('${log_date}','MONDAY'),-7) AS monday_date,
    date_add(next_day('${log_date}','MONDAY'),-1) AS sunday_date,
    concat(
        date_add(next_day('${log_date}','MONDAY'),-7),'-',
        date_add(next_day('${log_date}','MONDAY'),-1)) AS wk_dt
FROM ${warehouse}.dws_uv_detail_day
WHERE 
    dt>=date_add(next_day('${log_date}','MONDAY'),-7)
AND 
    dt<=date_add(next_day('${log_date}','MONDAY'),-1)
GROUP BY mid_id;

CREATE TABLE IF NOT EXISTS ${warehouse}.dws_uv_detail_mn(
    mid_id string COMMENT '设备唯一标识',
    user_id string COMMENT '用户标识',
    version_code string COMMENT '程序版本号',
    version_name string COMMENT '程序版本名',
    lang string COMMENT '系统语言',
    source string COMMENT '渠道号',
    os string COMMENT '手机操作系统',
    area string COMMENT '区域',
    model string COMMENT '手机型号',
    brand string COMMENT '手机品牌',
    sdk_version string COMMENT '开发包版本',
    gmail string COMMENT '邮箱',
    height_width string COMMENT '屏幕高宽',
    app_time string COMMENT '客户端日志产生的时间',
    network string COMMENT '网络模式',
    lng string COMMENT '经度',
    lat string COMMENT '纬度'
) COMMENT '月活用户明细表'
PARTITIONED BY (mn string);

INSERT OVERWRITE TABLE ${warehouse}.dws_uv_detail_mn
PARTITION(mn)
SELECT
    mid_id,
    concat_ws(',',collect_set(user_id)) AS user_id,
    concat_ws(',',collect_set(version_code)) AS version_code,
    concat_ws(',',collect_set(version_name)) AS version_name,
    concat_ws(',',collect_set(lang)) AS lang,
    concat_ws(',',collect_set(source)) AS source,
    concat_ws(',',collect_set(os)) AS os,
    concat_ws(',',collect_set(area)) AS area,
    concat_ws(',',collect_set(model)) AS model,
    concat_ws(',',collect_set(brand)) AS brand,
    concat_ws(',',collect_set(sdk_version)) AS sdk_version,
    concat_ws(',',collect_set(gmail)) AS gmail,
    concat_ws(',',collect_set(height_width)) AS height_width,
    concat_ws(',',collect_set(app_time)) AS app_time,
    concat_ws(',',collect_set(network)) AS network,
    concat_ws(',',collect_set(lng)) AS lng,
    concat_ws(',',collect_set(lat)) AS lat,
    date_format('${log_date}','yyyy-MM') AS mn
FROM ${warehouse}.dws_uv_detail_day
WHERE date_format(dt,'yyyy-MM')=date_format('${log_date}','yyyy-MM')
GROUP BY mid_id;
"

# 登录hive client执行hive sql语句
ssh -T $user@$hive_client <<EOF
    $hive_bin_dir/hive -e "$hive_sql"
EOF

# 获取结束时间
end_time=$(date +%s)
# 计算执行时间
execute_time=$(($end_time - $start_time))
echo -e "\n----------Transport logs from dwd to dws hive table on [$host] takes $execute_time seconds----------\n"