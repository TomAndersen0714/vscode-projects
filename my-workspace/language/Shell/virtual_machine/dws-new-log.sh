#!/bin/bash
# 此脚本用于每天凌晨执行,主要功能是基于不同周期的活跃用户表
# (dws_new_mid_day/dws_uv_detail_wk/dws_uv_detail_mn)
# 生成不同周期的新增用户表
# 当未传入参数时,默认生成前一天对应的不同周期活跃用户表
# 当传入单个参数时,则生成指定日期的不同周期活跃用户表
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

CREATE EXTERNAL TABLE IF NOT EXISTS ${warehouse}.dws_new_mid_day(
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
) COMMENT '每日新增设备信息表'
PARTITIONED BY(dt string);

INSERT OVERWRITE TABLE ${warehouse}.dws_new_mid_day
PARTITION(dt)
SELECT
    uv_day.mid_id,
    uv_day.user_id,
    uv_day.version_code,
    uv_day.version_name,
    uv_day.lang,
    uv_day.source,
    uv_day.os,
    uv_day.area,
    uv_day.model,
    uv_day.brand,
    uv_day.sdk_version,
    uv_day.gmail,
    uv_day.height_width,
    uv_day.app_time,
    uv_day.network,
    uv_day.lng,
    uv_day.lat,
    '${log_date}' AS dt
FROM ${warehouse}.dws_uv_detail_day AS uv_day
LEFT JOIN (
    SELECT *
    FROM ${warehouse}.dws_new_mid_day
    WHERE dt<'${log_date}'
) AS nm_day
ON uv_day.mid_id = nm_day.mid_id
WHERE uv_day.dt='${log_date}'
AND nm_day.mid_id IS NULL;


CREATE EXTERNAL TABLE IF NOT EXISTS ${warehouse}.dws_new_mid_wk(
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
) COMMENT '每周新增设备信息表'
PARTITIONED BY(week string);

INSERT OVERWRITE TABLE ${warehouse}.dws_new_mid_wk
PARTITION(week) 
SELECT
    uv_wk.mid_id,
    uv_wk.user_id,
    uv_wk.version_code,
    uv_wk.version_name,
    uv_wk.lang,
    uv_wk.source,
    uv_wk.os,
    uv_wk.area,
    uv_wk.model,
    uv_wk.brand,
    uv_wk.sdk_version,
    uv_wk.gmail,
    uv_wk.height_width,
    uv_wk.app_time,
    uv_wk.network,
    uv_wk.lng,
    uv_wk.lat,
    uv_wk.monday_date,
    uv_wk.sunday_date,
    uv_wk.week
FROM ${warehouse}.dws_uv_detail_wk AS uv_wk
LEFT JOIN (
    SELECT *
    FROM ${warehouse}.dws_new_mid_wk
    WHERE sunday_date<'${log_date}'
) AS nm_wk
ON uv_wk.mid_id = nm_wk.mid_id
WHERE uv_wk.week=concat(date_add(next_day('${log_date}','MONDAY'),-7),
    '-',date_add(next_day('${log_date}','MONDAY'),-1))
AND nm_wk.mid_id IS NULL;

CREATE EXTERNAL TABLE IF NOT EXISTS ${warehouse}.dws_new_mid_mn(
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
)
PARTITIONED BY(month string);

INSERT OVERWRITE TABLE ${warehouse}.dws_new_mid_mn
PARTITION(month)
SELECT
    uv_mn.mid_id,
    uv_mn.user_id,
    uv_mn.version_code,
    uv_mn.version_name,
    uv_mn.lang,
    uv_mn.source,
    uv_mn.os,
    uv_mn.area,
    uv_mn.model,
    uv_mn.brand,
    uv_mn.sdk_version,
    uv_mn.gmail,
    uv_mn.height_width,
    uv_mn.app_time,
    uv_mn.network,
    uv_mn.lng,
    uv_mn.lat,
    uv_mn.month
FROM ${warehouse}.dws_uv_detail_mn AS uv_mn
LEFT JOIN (
    SELECT *
    FROM ${warehouse}.dws_new_mid_mn
    WHERE month<date_format('${log_date}','yyyy-MM')
) AS nm_mn
ON uv_mn.mid_id = nm_mn.mid_id
WHERE uv_mn.month = date_format('${log_date}','yyyy-MM');

"

# 登录hive client执行hive sql语句
ssh -T $user@$hive_client <<EOF
    $hive_bin_dir/hive -e "$hive_sql"
EOF

# 获取结束时间
end_time=$(date +%s)
# 计算执行时间
execute_time=$(($end_time - $start_time))
echo -e "\n----------Transport logs from dws_uv to dws_new_mid hive table on [$host] takes $execute_time seconds----------\n"