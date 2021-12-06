#!/bin/bash
# 此脚本用于每天凌晨执行
# 当未传入参数时,默认将前一天ods层中收集的日志数据处理后传输到dwd层的Hive表中
# 当传入单个参数时,将ods层中指定日期的日志数据处理后传输到dwd层的Hive表中
# 本脚本最多只支持传入单个参数,且必须为特定格式日期,即:yyyy-MM-dd,如:2020-06-22

# 判断参数个数,只支持传入单个参数
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

# 用于执行的hive-sql语句
hive_sql="
CREATE EXTERNAL TABLE IF NOT EXISTS ${warehouse}.dwd_start_log(
    mid_id string COMMENT '设备唯一标识',
    user_id string COMMENT '用户标识',
    version_code string COMMENT 'versionCode,程序版本号',
    version_name string COMMENT 'versionName,程序版本名',
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
    entry string COMMENT '页面入口来源',
    open_ad_type string COMMENT '开屏广告类型',
    action string COMMENT '动作行为编号',
    loading_time string COMMENT '加载时长',
    detail string COMMENT '细节信息',
    extend1 string COMMENT '扩展信息1'
)
PARTITIONED BY (dt string);

set hive.execution.engine=tez;

INSERT INTO TABLE ${warehouse}.dwd_start_log
PARTITION(dt='${log_date}')
SELECT
    v_tbl.*
FROM ${warehouse}.ods_start_log
LATERAL VIEW
json_tuple(line,'mid','uid','vc','vn','l','sr','os',
    'ar','md','ba','sv','g','hw','t','nw','ln','la','entry',
    'open_ad_type','action','loading_time','detail','extend1') v_tbl 
    AS 
    mid_id,user_id,version_code,version_name,lang,source,os,
    area,model,brand,sdk_version,gmail,height_width,app_time,
    network,lng,lat,entry,open_ad_type,action,loading_time,
    detail,extend1
WHERE dt='${log_date}';

CREATE TABLE IF NOT EXISTS ${warehouse}.dwd_event_log(
    mid_id string COMMENT '设备唯一标识',
    user_id string COMMENT '用户标识',
    version_code string COMMENT 'versionCode,程序版本号',
    version_name string COMMENT 'versionName,程序版本名',
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
    event_name string COMMENT '事件名',
    event_json string COMMENT '事件详情JSON字符串',
    server_time string COMMENT '事件发生时间'
)
PARTITIONED BY(dt string);

drop temporary function if exists event_log_handler;
create temporary function event_log_handler as 'com.tomandersen.hive.udtfs.CustomUDTFEventLogHandler'
using jar '/opt/libs/hive-custom-component-1.0.jar';

INSERT INTO TABLE ${warehouse}.dwd_event_log
PARTITION(dt='${log_date}')
SELECT
    mid_id,user_id,version_code,version_name,lang,source,os,area,model,
    brand,sdk_version,gmail,height_width,app_time,network,lng,lat,
    event_name,event_json,server_time
FROM (
    SELECT event_log_handler(line,'mid','uid','vc','vn','l','sr','os',
        'ar','md','ba','sv','g','hw','t','nw','ln','la') 
        AS 
        (mid_id,user_id,version_code,version_name,lang,source,os,area,model,
        brand,sdk_version,gmail,height_width,app_time,network,lng,lat,
        server_time,event_name,event_json)
    FROM ${warehouse}.ods_event_log
    WHERE dt='${log_date}'
) AS tbl_1;
"

# 登录hive client执行hive sql语句
ssh -T $user@$hive_client <<EOF
    $hive_bin_dir/hive -e "$hive_sql"
EOF

# 获取结束时间
end_time=$(date +%s)
# 计算执行时间
execute_time=$(($end_time - $start_time))
echo -e "\n----------Transport logs from ods to dwd hive table on [$host] takes $execute_time seconds----------\n"