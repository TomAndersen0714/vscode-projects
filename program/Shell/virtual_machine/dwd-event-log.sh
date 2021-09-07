#!/bin/bash
# 此脚本用于每天凌晨执行
# 当未传入参数时,默认将前一天 dwd_event_log 表中的数据进行处理,分离各种事件表
# 当传入单个参数时,获取 dwd_event_log 表中的制定日期的数据进行处理,分离各种事件表
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

# 用于执行的hive-sql语句
hive_sql="
set hive.execution.engine=tez;

CREATE EXTERNAL TABLE IF NOT EXISTS ${warehouse}.dwd_display_log(
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
    action string COMMENT '动作行为编号',
    goodsid string COMMENT '商品编号',
    place string COMMENT '地区',
    extend1 string COMMENT '扩展信息1',
    category string COMMENT '商品类别',
    server_time string COMMENT '事件发生时间'
)
COMMENT '商品点击表'
PARTITIONED BY (dt string);

INSERT OVERWRITE TABLE ${warehouse}.dwd_display_log
PARTITION (dt='${log_date}')
SELECT
    mid_id,user_id,version_code,version_name,
    lang,source,os,area,
    model,brand,sdk_version,gmail,
    height_width,app_time,network,lng,lat,
    get_json_object(event_json,'$.kv.action') AS action,
    get_json_object(event_json,'$.kv.goodsid') AS goodsid,
    get_json_object(event_json,'$.kv.place') AS place,
    get_json_object(event_json,'$.kv.extend1') AS extend1,
    get_json_object(event_json,'$.kv.category') AS category,
    server_time
FROM ${warehouse}.dwd_event_log
WHERE dt='${log_date}' AND event_name='display';


CREATE TABLE IF NOT EXISTS ${warehouse}.dwd_newsdetail_log(
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
    entry string COMMENT '页面入口来源',
    action string COMMENT '动作行为编号',
    goodsid string COMMENT '商品编号',
    showtype string COMMENT '商品样式',
    news_staytime string COMMENT '页面停留时长',
    loading_time string COMMENT '加载时长',
    type1 string COMMENT '加载失败码',
    category string COMMENT '商品类别',
    server_time string COMMENT '事件发生时间'
)
COMMENT '商品详情页表'
PARTITIONED BY (dt string);

INSERT OVERWRITE TABLE ${warehouse}.dwd_newsdetail_log
PARTITION (dt='${log_date}')
SELECT
    mid_id,user_id,version_code,version_name,
    lang,source,os,area,
    model,brand,sdk_version,gmail,
    height_width,app_time,network,lng,lat,
    get_json_object(event_json,'$.kv.entry') AS entry,
    get_json_object(event_json,'$.kv.action') AS action,
    get_json_object(event_json,'$.kv.goodsid') AS goodsid,
    get_json_object(event_json,'$.kv.showtype') AS showtype,
    get_json_object(event_json,'$.kv.news_staytime') AS news_staytime,
    get_json_object(event_json,'$.kv.loading_time') AS loading_time,
    get_json_object(event_json,'$.kv.type1') AS type1,
    get_json_object(event_json,'$.kv.category') AS category,
    server_time
FROM ${warehouse}.dwd_event_log
WHERE dt='${log_date}' AND event_name='display';


CREATE TABLE IF NOT EXISTS ${warehouse}.dwd_loading_log(
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
    action string COMMENT '动作行为编号',
    loading_time string COMMENT '加载时长',
    loading_way string COMMENT '加载方式',
    extend1 string COMMENT '扩展信息1',
    extend2 string COMMENT '扩展信息2',
    type string COMMENT '加载类型',
    type1 string COMMENT '加载失败码',
    server_time string COMMENT '事件发生时间'
)
COMMENT '商品列表页加载事件表'
PARTITIONED BY (dt string);

INSERT OVERWRITE TABLE ${warehouse}.dwd_loading_log
PARTITION (dt='${log_date}')
SELECT
    mid_id,user_id,version_code,version_name,
    lang,source,os,area,
    model,brand,sdk_version,gmail,
    height_width,app_time,network,lng,lat,
    get_json_object(event_json,'$.kv.action') AS action,
    get_json_object(event_json,'$.kv.loading_time') AS loading_time,
    get_json_object(event_json,'$.kv.loading_way') AS loading_way,
    get_json_object(event_json,'$.kv.extend1') AS extend1,
    get_json_object(event_json,'$.kv.extend2') AS extend2,
    get_json_object(event_json,'$.kv.type') AS type,
    get_json_object(event_json,'$.kv.type1') AS type1,
    server_time
FROM ${warehouse}.dwd_event_log
WHERE dt='${log_date}' AND event_name='loading';


CREATE TABLE IF NOT EXISTS ${warehouse}.dwd_ad_log(
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
    entry string COMMENT '页面入口来源',
    action string COMMENT '动作行为编号',
    content string COMMENT '内容是否加载成功',
    show_style string COMMENT '内容样式',
    detail string COMMENT '失败码,成功则为空',
    ad_source string COMMENT '广告来源',
    behavior string COMMENT '用户行为(主动或被动)',
    newstype string COMMENT '广告类型',
    server_time string COMMENT '事件发生时间'
)
COMMENT '广告表'
PARTITIONED BY(dt string);

INSERT OVERWRITE TABLE ${warehouse}.dwd_ad_log
PARTITION (dt='${log_date}')
SELECT
    mid_id,user_id,version_code,version_name,
    lang,source,os,area,
    model,brand,sdk_version,gmail,
    height_width,app_time,network,lng,lat,
    get_json_object(event_json,'$.kv.entry') AS entry,
    get_json_object(event_json,'$.kv.action') AS action,
    get_json_object(event_json,'$.kv.content') AS content,
    get_json_object(event_json,'$.kv.show_style') AS show_style,
    get_json_object(event_json,'$.kv.detail') AS detail,
    get_json_object(event_json,'$.kv.source') AS ad_source,
    get_json_object(event_json,'$.kv.behavior') AS behavior,
    get_json_object(event_json,'$.kv.newstype') AS newstype,
    server_time
FROM ${warehouse}.dwd_event_log
WHERE dt='${log_date}' AND event_name='ad';

CREATE TABLE IF NOT EXISTS ${warehouse}.dwd_notification_log(
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
    action string COMMENT '动作行为编号',
    noti_type string COMMENT '通知类型',
    ap_time string COMMENT '客户端弹出时间',
    content string COMMENT '内容是否加载成功',
    server_time string COMMENT '事件发生时间'
)
COMMENT '消息通知表'
PARTITIONED BY(dt string);

INSERT OVERWRITE TABLE ${warehouse}.dwd_notification_log
PARTITION (dt='${log_date}')
SELECT
    mid_id,user_id,version_code,version_name,
    lang,source,os,area,
    model,brand,sdk_version,gmail,
    height_width,app_time,network,lng,lat,
    get_json_object(event_json,'$.kv.action') AS action,
    get_json_object(event_json,'$.kv.type') AS noti_type,
    get_json_object(event_json,'$.kv.ap_time') AS ap_time,
    get_json_object(event_json,'$.kv.content') AS content,
    server_time
FROM ${warehouse}.dwd_event_log
WHERE dt='${log_date}' AND event_name='notification';


CREATE TABLE IF NOT EXISTS ${warehouse}.dwd_active_foreground_log(
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
    push_id string COMMENT '推送消息ID,如果不是从推送消息打开,则为空',
    access string COMMENT '客户端打开方式',
    server_time string COMMENT '事件发生时间'
)
COMMENT '客户端前台活跃表'
PARTITIONED BY(dt string);

INSERT OVERWRITE TABLE ${warehouse}.dwd_active_foreground_log
PARTITION (dt='${log_date}')
SELECT
    mid_id,user_id,version_code,version_name,
    lang,source,os,area,
    model,brand,sdk_version,gmail,
    height_width,app_time,network,lng,lat,
    get_json_object(event_json,'$.kv.push_id') AS push_id,
    get_json_object(event_json,'$.kv.access') AS access,
    server_time
FROM ${warehouse}.dwd_event_log
WHERE dt='${log_date}' AND event_name='active_foreground';


CREATE TABLE IF NOT EXISTS ${warehouse}.dwd_active_background_log(
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
    active_source string COMMENT '后台行为',
    server_time string COMMENT '事件发生时间'
)
COMMENT '客户端后台活跃表'
PARTITIONED BY(dt string);

INSERT OVERWRITE TABLE ${warehouse}.dwd_active_background_log
PARTITION (dt='${log_date}')
SELECT
    mid_id,user_id,version_code,version_name,
    lang,source,os,area,
    model,brand,sdk_version,gmail,
    height_width,app_time,network,lng,lat,
    get_json_object(event_json,'$.kv.active_source') AS active_source,
    server_time
FROM ${warehouse}.dwd_event_log
WHERE dt='${log_date}' AND event_name='active_background';


CREATE TABLE IF NOT EXISTS ${warehouse}.dwd_comment_log(
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
    comment_id string COMMENT '评论ID',
    userid string COMMENT '评论用户ID',
    p_comment_id string COMMENT '父级评论ID',
    content string COMMENT '评论内容',
    addtime string COMMENT '创建时间',
    other_id string COMMENT '相关评论ID',
    praise_count string COMMENT '评论点赞数量',
    reply_count string COMMENT '评论回复数量',
    server_time string COMMENT '事件发生时间'
)
COMMENT '商品评论表'
PARTITIONED BY(dt string);

INSERT OVERWRITE TABLE ${warehouse}.dwd_comment_log
PARTITION (dt='${log_date}')
SELECT
    mid_id,user_id,version_code,version_name,
    lang,source,os,area,
    model,brand,sdk_version,gmail,
    height_width,app_time,network,lng,lat,
    get_json_object(event_json,'$.kv.comment_id') AS comment_id,
    get_json_object(event_json,'$.kv.userid') AS userid,
    get_json_object(event_json,'$.kv.p_comment_id') AS p_comment_id,
    get_json_object(event_json,'$.kv.content') AS content,
    get_json_object(event_json,'$.kv.addtime') AS addtime,
    get_json_object(event_json,'$.kv.other_id') AS other_id,
    get_json_object(event_json,'$.kv.praise_count') AS praise_count,
    get_json_object(event_json,'$.kv.reply_count') AS reply_count,
    server_time
FROM ${warehouse}.dwd_event_log
WHERE dt='${log_date}' AND event_name='comment';


CREATE TABLE IF NOT EXISTS ${warehouse}.dwd_favorites_log(
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
    id string COMMENT '收藏ID',
    course_id string COMMENT '商品ID',
    userid string COMMENT '用户ID',
    add_time string COMMENT '创建时间',
    server_time string COMMENT '事件发生时间'
)
COMMENT '商品收藏表'
PARTITIONED BY(dt string);

INSERT OVERWRITE TABLE ${warehouse}.dwd_favorites_log
PARTITION (dt='${log_date}')
SELECT
    mid_id,user_id,version_code,version_name,
    lang,source,os,area,
    model,brand,sdk_version,gmail,
    height_width,app_time,network,lng,lat,
    get_json_object(event_json,'$.kv.id') AS id,
    get_json_object(event_json,'$.kv.course_id') AS course_id,
    get_json_object(event_json,'$.kv.userid') AS userid,
    get_json_object(event_json,'$.kv.add_time') AS add_time,
    server_time
FROM ${warehouse}.dwd_event_log
WHERE dt='${log_date}' AND event_name='favorites';


CREATE TABLE IF NOT EXISTS ${warehouse}.dwd_praise_log(
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
    id string COMMENT '点赞记录ID',
    userid string COMMENT '用户ID',
    target_id string COMMENT '点赞对象ID',
    type string COMMENT '点赞类型',
    add_time string COMMENT '添加事件',
    server_time string COMMENT '事件发生时间'
)
COMMENT '商品点赞表'
PARTITIONED BY(dt string);

INSERT OVERWRITE TABLE ${warehouse}.dwd_praise_log
PARTITION (dt='${log_date}')
SELECT
    mid_id,user_id,version_code,version_name,
    lang,source,os,area,
    model,brand,sdk_version,gmail,
    height_width,app_time,network,lng,lat,
    get_json_object(event_json,'$.kv.id') AS id,
    get_json_object(event_json,'$.kv.userid') AS userid,
    get_json_object(event_json,'$.kv.target_id') AS target_id,
    get_json_object(event_json,'$.kv.type') AS type,
    get_json_object(event_json,'$.kv.add_time') AS add_time,
    server_time
FROM ${warehouse}.dwd_event_log
WHERE dt='${log_date}' AND event_name='praise';


CREATE TABLE IF NOT EXISTS ${warehouse}.dwd_error_log(
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
    errorBrief string COMMENT '错误摘要',
    errorDetail string COMMENT '错误详情',
    server_time string COMMENT '事件发生时间'
)
COMMENT '错误日志表'
PARTITIONED BY(dt string);

INSERT OVERWRITE TABLE ${warehouse}.dwd_error_log
PARTITION (dt='${log_date}')
SELECT
    mid_id,user_id,version_code,version_name,
    lang,source,os,area,
    model,brand,sdk_version,gmail,
    height_width,app_time,network,lng,lat,
    get_json_object(event_json,'$.kv.errorBrief') AS errorBrief,
    get_json_object(event_json,'$.kv.errorDetail') AS errorDetail,
    server_time
FROM ${warehouse}.dwd_event_log
WHERE dt='${log_date}' AND event_name='error';
"

# 登录hive client执行hive sql语句
ssh -T $user@$hive_client <<EOF
    $hive_bin_dir/hive -e "$hive_sql"
EOF

# 获取结束时间
end_time=$(date +%s)
# 计算执行时间
execute_time=$(($end_time - $start_time))
echo -e "\n----------Handle hive dwd-event-log table on [$host] takes $execute_time seconds----------\n"