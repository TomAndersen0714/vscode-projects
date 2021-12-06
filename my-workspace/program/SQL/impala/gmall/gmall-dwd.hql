-- DWD层建表语句
-- 使用Tez引擎处理所有的查询语句
set hive.execution.engine=tez;
-- 删除已存在表
DROP TABLE IF EXISTS gmall.dwd_start_log;
-- 创建外部表,启动日志表dwd_start_log
CREATE EXTERNAL TABLE IF NOT EXISTS gmall.dwd_start_log(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '手机操作系统',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT '开发包版本',
    `gmail` string COMMENT '邮箱',
    `height_width` string COMMENT '屏幕高宽',
    `app_time` string COMMENT '客户端日志产生的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `entry` string COMMENT '页面入口来源',
    `open_ad_type` string COMMENT '开屏广告类型',
    `action` string COMMENT '动作行为编号',
    `loading_time` string COMMENT '加载时长',
    `detail` string COMMENT '失败码,成功则为空',
    `extend1` string COMMENT '扩展信息1'
)
PARTITIONED BY (dt string)
-- STORED AS PARQUET -- 使用PARQUET文件格式存储表数据
;

-- 向表中导入数据,导入表 ods_start_log 的各个Json Key的查询结果
-- 方法一:
-- 使用内建函数UDTF函数 get_json_object,需要创建多个UDTF实例
INSERT OVERWRITE TABLE dwd_start_log
PARTITION(dt='2020-06-12')
SELECT
    get_json_object(ods_start_log.line,'$.mid') AS mid_id,
    get_json_object(ods_start_log.line,'$.uid') AS user_id,
    get_json_object(ods_start_log.line,'$.vc') AS version_code,
    get_json_object(ods_start_log.line,'$.vn') AS version_name,
    get_json_object(ods_start_log.line,'$.l') AS lang,
    get_json_object(ods_start_log.line,'$.sr') AS source,
    get_json_object(ods_start_log.line,'$.os') AS os,
    get_json_object(ods_start_log.line,'$.ar') AS area,
    get_json_object(ods_start_log.line,'$.md') AS model,
    get_json_object(ods_start_log.line,'$.ba') AS brand,
    get_json_object(ods_start_log.line,'$.sv') AS sdk_version,
    get_json_object(ods_start_log.line,'$.g') AS gmail,
    get_json_object(ods_start_log.line,'$.hw') AS height_width,
    get_json_object(ods_start_log.line,'$.t') AS app_time,
    get_json_object(ods_start_log.line,'$.nw') AS network,
    get_json_object(ods_start_log.line,'$.ln') AS lng,
    get_json_object(ods_start_log.line,'$.la') AS lat,
    get_json_object(ods_start_log.line,'$.entry') AS entry,
    get_json_object(ods_start_log.line,'$.open_ad_type') AS open_ad_type,
    get_json_object(ods_start_log.line,'$.action') AS action,
    get_json_object(ods_start_log.line,'$.loading_time') AS loading_time,
    get_json_object(ods_start_log.line,'$.detail') AS detail,
    get_json_object(ods_start_log.line,'$.extend1') AS extend1
FROM ods_start_log
WHERE dt='2020-06-12';
-- 方法二(更高效):
-- 使用内建UDTF函数 json_tuple,只需要创建单个UDTF实例
INSERT OVERWRITE TABLE dwd_start_log
PARTITION(dt='2020-06-12')
SELECT
    v_tbl.*
FROM ods_start_log
LATERAL VIEW
json_tuple(line,'mid','uid','vc','vn','l','sr','os',
    'ar','md','ba','sv','g','hw','t','nw','ln','la','entry',
    'open_ad_type','action','loading_time','detail','extend1') v_tbl 
    AS 
    mid_id,user_id,version_code,version_name,lang,source,os,
    area,model,brand,sdk_version,gmail,height_width,app_time,
    network,lng,lat,entry,open_ad_type,action,loading_time,
    detail,extend1
WHERE dt='2020-06-12';
-- 方法三:
-- 方法二的另一种写法,不使用侧视图,直接调用UDTF
INSERT OVERWRITE TABLE dwd_start_log
PARTITION(dt='2020-06-12')
SELECT
    json_tuple(line,'mid','uid','vc','vn','l','sr','os',
    'ar','md','ba','sv','g','hw','t','nw','ln','la','entry',
    'open_ad_type','action','loading_time','detail','extend1')
    AS 
    (mid_id,user_id,version_code,version_name,lang,source,os,
    area,model,brand,sdk_version,gmail,height_width,app_time,
    network,lng,lat,entry,open_ad_type,action,loading_time,
    detail,extend1)
FROM ods_start_log
WHERE dt='2020-06-12';
-- 方法四:
-- 创建自定义的UDTF函数 start_log_handler,专门用于处理启动日志 start_log
-- 原理和 json_tuple 相同,都是从Json对象中选取与Json Key对应的Json Value
-- 创建临时UDTF函数 start_log_handler
drop temporary function if exists start_log_handler;
create temporary function start_log_handler as 'com.tomandersen.hive.udtfs.CustomUDTFStartLogHandler'
using jar '/opt/libs/hive-custom-component-1.0.jar';
-- 使用自定义的UDTF函数处理启动日志 ods_start_log,效率没有内建UDTF函数 json_tuple 高
-- 使用示例:SELECT start_log_handler(line,'md') from gmall.ods_start_log;
INSERT OVERWRITE TABLE dwd_start_log
PARTITION(dt='2020-06-12')
SELECT
    start_log_handler(line,'mid','uid','vc','vn','l','sr','os',
    'ar','md','ba','sv','g','hw','t','nw','ln','la','entry',
    'open_ad_type','action','loading_time','detail','extend1') 
    AS
    (mid_id,user_id,version_code,version_name,lang,source,os,
    area,model,brand,sdk_version,gmail,height_width,app_time,
    network,lng,lat,entry,open_ad_type,action,loading_time,
    detail,extend1)
FROM gmall.ods_start_log
WHERE dt='2020-06-12';


-- 创建事件日志基础明细表 dwd_event_log
-- 此表用于记录格式化后的事件日志
DROP TABLE IF EXISTS gmall.dwd_event_log;
CREATE TABLE IF NOT EXISTS gmall.dwd_event_log(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '手机操作系统',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT '开发包版本',
    `gmail` string COMMENT '邮箱',
    `height_width` string COMMENT '屏幕高宽',
    `app_time` string COMMENT '客户端日志产生的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度', 
    `event_name` string COMMENT '事件名',
    `event_json` string COMMENT '事件详情JSON字符串',
    `server_time` string COMMENT '事件发生时间'
)
PARTITIONED BY(dt string)
-- STORED AS PARQUET -- 保存为PARQUET文件格式
;

-- 向表 dwd_event_log 中导入数据
-- 方法一:
-- 使用自定义UDTF函数 event_log_handler
-- 创建临时UDTF函数 event_log_handler
drop temporary function if exists event_log_handler;
create temporary function event_log_handler as 'com.tomandersen.hive.udtfs.CustomUDTFEventLogHandler'
using jar '/opt/libs/hive-custom-component-1.0.jar';
-- 使用临时UDTF函数 event_log_handler 处理 ods_event_log 表数据
set hive.execution.engine=tez;
INSERT OVERWRITE TABLE dwd_event_log
PARTITION(dt='2020-06-12')
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
    FROM gmall.ods_event_log
    WHERE dt='2020-06-12'
) AS tbl_1;
-- 方法二:
-- 使用自定义临时UDTF函数 event_log_base_handler 和 event_log_event_handler
-- 创建 ods_event_log 预处理临时函数 event_log_base_handler
-- 可以获取事件日志中的任何Json Value,同时输出server_time
drop temporary function if exists event_log_base_handler;
create temporary function event_log_base_handler as 'com.tomandersen.hive.udtfs.CustomUDTFEventLogBaseHander'
using jar '/opt/libs/hive-custom-component-1.0.jar';
-- 创建 ods_event_log 事件字段et处理临时函数 event_log_event_handler
-- 可以用于处理事件JSON数组,输出其中的所有事件
drop temporary function if exists event_log_event_handler;
create temporary function event_log_event_handler as 'com.tomandersen.hive.udtfs.CustomUDTFEventLogEventHandler'
using jar '/opt/libs/hive-custom-component-1.0.jar';
-- 创建 ods_event_log 公共字段处理临时函数 event_log_common_handler
-- 可以用于处理事件日志的公共字段,输出其中的任意Json Value
drop temporary function if exists event_log_common_handler;
create temporary function event_log_common_handler as 'com.tomandersen.hive.udtfs.CustomUDTFEventLogCommonHandler'
using jar '/opt/libs/hive-custom-component-1.0.jar';
-- 调用UDTF函数 event_log_base_handler,event_log_common_handler,event_log_event_handler
set hive.execution.engine=tez;
INSERT OVERWRITE TABLE dwd_event_log
PARTITION(dt='2020-06-12')
SELECT
    v_tbl_1.*,
    v_tbl_2.*,
    tbl_1.server_time
FROM (
    SELECT
        event_log_base_handler(line,'cm','et') AS (common_fields_json,events_json,server_time)
    FROM gmall.ods_event_log
    WHERE dt='2020-06-12'
) AS tbl_1
LATERAL VIEW event_log_common_handler(common_fields_json,'mid','uid','vc','vn','l','sr','os',
        'ar','md','ba','sv','g','hw','t','nw','ln','la') v_tbl_1 AS mid_id,user_id,version_code,version_name,lang,source,os,area,model,
        brand,sdk_version,gmail,height_width,app_time,network,lng,lat
LATERAL VIEW event_log_event_handler(events_json) v_tbl_2 AS event_name,event_json;


-- 创建商品点击表 dwd_display_log
set hive.execution.engine=tez;
DROP TABLE IF EXISTS gmall.dwd_display_log;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall.dwd_display_log(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '手机操作系统',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT '开发包版本',
    `gmail` string COMMENT '邮箱',
    `height_width` string COMMENT '屏幕高宽',
    `app_time` string COMMENT '客户端日志产生的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `action` string COMMENT '动作行为编号',
    `goodsid` string COMMENT '商品编号',
    `place` string COMMENT '地区',
    `extend1` string COMMENT '扩展信息1',
    `category` string COMMENT '商品类别',
    `server_time` string COMMENT '事件发生时间'
)
COMMENT '商品点击表'
PARTITIONED BY (dt string);

-- 向商品点击表 dwd_display_log 中导入数据
INSERT OVERWRITE TABLE gmall.dwd_display_log
PARTITION (dt='2020-06-22')
SELECT
    `mid_id`,`user_id`,`version_code`,`version_name`,
    `lang`,`source`,`os`,`area`,
    `model`,`brand`,`sdk_version`,`gmail`,
    `height_width`,`app_time`,`network`,`lng`,`lat`,
    get_json_object(event_json,'$.kv.action') AS action,
    get_json_object(event_json,'$.kv.goodsid') AS goodsid,
    get_json_object(event_json,'$.kv.place') AS place,
    get_json_object(event_json,'$.kv.extend1') AS extend1,
    get_json_object(event_json,'$.kv.category') AS category,
    server_time
FROM gmall.dwd_event_log
WHERE dt='2020-06-22' AND event_name='display';

-- 查询表数据,验证结果
SELECT * FROM gmall.dwd_display_log LIMIT 2;


-- 创建商品详情页表 dwd_newsdetail_log
DROP TABLE IF EXISTS dwd_newsdetail_log;
CREATE TABLE IF NOT EXISTS dwd_newsdetail_log(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '手机操作系统',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT '开发包版本',
    `gmail` string COMMENT '邮箱',
    `height_width` string COMMENT '屏幕高宽',
    `app_time` string COMMENT '客户端日志产生的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `entry` string COMMENT '页面入口来源',
    `action` string COMMENT '动作行为编号',
    `goodsid` string COMMENT '商品编号',
    `showtype` string COMMENT '商品样式',
    `news_staytime` string COMMENT '页面停留时长',
    `loading_time` string COMMENT '加载时长',
    `type1` string COMMENT '加载失败码',
    `category` string COMMENT '商品类别',
    `server_time` string COMMENT '事件发生时间'
)
COMMENT '商品详情页表'
PARTITIONED BY (dt string);

-- 向商品详情页表 dwd_newsdetail_log 中导入数据
INSERT OVERWRITE TABLE gmall.dwd_newsdetail_log
PARTITION (dt='2020-06-22')
SELECT
    `mid_id`,`user_id`,`version_code`,`version_name`,
    `lang`,`source`,`os`,`area`,
    `model`,`brand`,`sdk_version`,`gmail`,
    `height_width`,`app_time`,`network`,`lng`,`lat`,
    get_json_object(event_json,'$.kv.entry') AS entry,
    get_json_object(event_json,'$.kv.action') AS action,
    get_json_object(event_json,'$.kv.goodsid') AS goodsid,
    get_json_object(event_json,'$.kv.showtype') AS showtype,
    get_json_object(event_json,'$.kv.news_staytime') AS news_staytime,
    get_json_object(event_json,'$.kv.loading_time') AS loading_time,
    get_json_object(event_json,'$.kv.type1') AS type1,
    get_json_object(event_json,'$.kv.category') AS category,
    server_time
FROM gmall.dwd_event_log
WHERE dt='2020-06-22' AND event_name='display';


-- 创建商品列表页加载事件表 dwd_loading_log
CREATE TABLE IF EXISTS dwd_loading_log;
CREATE TABLE IF NOT EXISTS dwd_loading_log(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '手机操作系统',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT '开发包版本',
    `gmail` string COMMENT '邮箱',
    `height_width` string COMMENT '屏幕高宽',
    `app_time` string COMMENT '客户端日志产生的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `action` string COMMENT '动作行为编号',
    `loading_time` string COMMENT '加载时长',
    `loading_way` string COMMENT '加载方式',
    `extend1` string COMMENT '扩展信息1',
    `extend2` string COMMENT '扩展信息2',
    `type` string COMMENT '加载类型',
    `type1` string COMMENT '加载失败码',
    `server_time` string COMMENT '事件发生时间'
)
COMMENT '商品列表页加载事件表'
PARTITIONED BY (dt string);

-- 向商品加载事件表 dwd_loading_log 中填充数据
INSERT OVERWRITE TABLE dwd_loading_log
PARTITION (dt='2020-06-22')
SELECT
    `mid_id`,`user_id`,`version_code`,`version_name`,
    `lang`,`source`,`os`,`area`,
    `model`,`brand`,`sdk_version`,`gmail`,
    `height_width`,`app_time`,`network`,`lng`,`lat`,
    get_json_object(event_json,'$.kv.action') AS action,
    get_json_object(event_json,'$.kv.loading_time') AS loading_time,
    get_json_object(event_json,'$.kv.loading_way') AS loading_way,
    get_json_object(event_json,'$.kv.extend1') AS extend1,
    get_json_object(event_json,'$.kv.extend2') AS extend2,
    get_json_object(event_json,'$.kv.type') AS type,
    get_json_object(event_json,'$.kv.type1') AS type1,
    server_time
FROM gmall.dwd_event_log
WHERE dt='2020-06-22' AND event_name='loading';

-- 创建广告表 dwd_ad_log
DROP TABLE IF EXISTS dwd_ad_log;
CREATE TABLE IF NOT EXISTS dwd_ad_log(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '手机操作系统',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT '开发包版本',
    `gmail` string COMMENT '邮箱',
    `height_width` string COMMENT '屏幕高宽',
    `app_time` string COMMENT '客户端日志产生的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `entry` string COMMENT '页面入口来源',
    `action` string COMMENT '动作行为编号',
    `content` string COMMENT '内容是否加载成功',
    `show_style` string COMMENT '内容样式',
    `detail` string COMMENT '失败码,成功则为空',
    `ad_source` string COMMENT '广告来源',
    `behavior` string COMMENT '用户行为(主动或被动)',
    `newstype` string COMMENT '广告类型',
    `server_time` string COMMENT '事件发生时间'
)
COMMENT '广告表'
PARTITIONED BY(dt string);

-- 向广告表 dwd_ad_log 中填充数据
INSERT OVERWRITE TABLE dwd_ad_log
PARTITION (dt='2020-06-22')
SELECT
    `mid_id`,`user_id`,`version_code`,`version_name`,
    `lang`,`source`,`os`,`area`,
    `model`,`brand`,`sdk_version`,`gmail`,
    `height_width`,`app_time`,`network`,`lng`,`lat`,
    get_json_object(event_json,'$.kv.entry') AS entry,
    get_json_object(event_json,'$.kv.action') AS action,
    get_json_object(event_json,'$.kv.content') AS content,
    get_json_object(event_json,'$.kv.show_style') AS show_style,
    get_json_object(event_json,'$.kv.detail') AS detail,
    get_json_object(event_json,'$.kv.source') AS ad_source,
    get_json_object(event_json,'$.kv.behavior') AS behavior,
    get_json_object(event_json,'$.kv.newstype') AS newstype,
    server_time
FROM gmall.dwd_event_log
WHERE dt='2020-06-22' AND event_name='ad';

-- 创建消息通知表 dwd_notification_log
DROP TABLE IF EXISTS dwd_notification_log;
CREATE TABLE IF NOT EXISTS dwd_notification_log(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '手机操作系统',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT '开发包版本',
    `gmail` string COMMENT '邮箱',
    `height_width` string COMMENT '屏幕高宽',
    `app_time` string COMMENT '客户端日志产生的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `action` string COMMENT '动作行为编号',
    `noti_type` string COMMENT '通知类型',
    `ap_time` string COMMENT '客户端弹出时间',
    `content` string COMMENT '内容是否加载成功',
    `server_time` string COMMENT '事件发生时间'
)
COMMENT '消息通知表'
PARTITIONED BY(dt string);

-- 向消息通知表 dwd_notification_log 中填充数据
INSERT OVERWRITE TABLE dwd_notification_log
PARTITION (dt='2020-06-22')
SELECT
    `mid_id`,`user_id`,`version_code`,`version_name`,
    `lang`,`source`,`os`,`area`,
    `model`,`brand`,`sdk_version`,`gmail`,
    `height_width`,`app_time`,`network`,`lng`,`lat`,
    get_json_object(event_json,'$.kv.action') AS action,
    get_json_object(event_json,'$.kv.type') AS noti_type,
    get_json_object(event_json,'$.kv.ap_time') AS ap_time,
    get_json_object(event_json,'$.kv.content') AS content,
    server_time
FROM gmall.dwd_event_log
WHERE dt='2020-06-22' AND event_name='notification';


-- 创建客户端前台活跃表 dwd_active_foreground_log 
DROP TABLE IF EXISTS dwd_active_foreground_log;
CREATE TABLE IF NOT EXISTS dwd_active_foreground_log(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '手机操作系统',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT '开发包版本',
    `gmail` string COMMENT '邮箱',
    `height_width` string COMMENT '屏幕高宽',
    `app_time` string COMMENT '客户端日志产生的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `push_id` string COMMENT '推送消息ID,如果不是从推送消息打开,则为空',
    `access` string COMMENT '客户端打开方式',
    `server_time` string COMMENT '事件发生时间'
)
COMMENT '客户端前台活跃表'
PARTITIONED BY(dt string);

-- 向客户端前台活跃表 dwd_active_foreground_log 中填充数据
INSERT OVERWRITE TABLE dwd_active_foreground_log
PARTITION (dt='2020-06-22')
SELECT
    `mid_id`,`user_id`,`version_code`,`version_name`,
    `lang`,`source`,`os`,`area`,
    `model`,`brand`,`sdk_version`,`gmail`,
    `height_width`,`app_time`,`network`,`lng`,`lat`,
    get_json_object(event_json,'$.kv.push_id') AS push_id,
    get_json_object(event_json,'$.kv.access') AS access,
    server_time
FROM gmall.dwd_event_log
WHERE dt='2020-06-22' AND event_name='active_foreground';


-- 创建客户端后台活跃表 dwd_active_background_log
DROP TABLE IF EXISTS dwd_active_background_log;
CREATE TABLE IF NOT EXISTS dwd_active_background_log(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '手机操作系统',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT '开发包版本',
    `gmail` string COMMENT '邮箱',
    `height_width` string COMMENT '屏幕高宽',
    `app_time` string COMMENT '客户端日志产生的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `active_source` string COMMENT '后台行为',
    `server_time` string COMMENT '事件发生时间'
)
COMMENT '客户端后台活跃表'
PARTITIONED BY(dt string);

-- 向客户端后台活跃表 dwd_active_background_log 中填充数据
INSERT OVERWRITE TABLE dwd_active_background_log
PARTITION (dt='2020-06-22')
SELECT
    `mid_id`,`user_id`,`version_code`,`version_name`,
    `lang`,`source`,`os`,`area`,
    `model`,`brand`,`sdk_version`,`gmail`,
    `height_width`,`app_time`,`network`,`lng`,`lat`,
    get_json_object(event_json,'$.kv.active_source') AS active_source,
    server_time
FROM gmall.dwd_event_log
WHERE dt='2020-06-22' AND event_name='active_background';


-- 创建商品评论表 dwd_comment_log
DROP TABLE IF EXISTS dwd_comment_log;
CREATE TABLE IF NOT EXISTS dwd_comment_log(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '手机操作系统',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT '开发包版本',
    `gmail` string COMMENT '邮箱',
    `height_width` string COMMENT '屏幕高宽',
    `app_time` string COMMENT '客户端日志产生的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `comment_id` string COMMENT '评论ID',
    `userid` string COMMENT '评论用户ID',
    `p_comment_id` string COMMENT '父级评论ID',
    `content` string COMMENT '评论内容',
    `addtime` string COMMENT '创建时间',
    `other_id` string COMMENT '相关评论ID',
    `praise_count` string COMMENT '评论点赞数量',
    `reply_count` string COMMENT '评论回复数量',
    `server_time` string COMMENT '事件发生时间'
)
COMMENT '商品评论表'
PARTITIONED BY(dt string);

-- 向商品评论表 dwd_comment_log 中填充数据
INSERT OVERWRITE TABLE dwd_comment_log
PARTITION (dt='2020-06-22')
SELECT
    `mid_id`,`user_id`,`version_code`,`version_name`,
    `lang`,`source`,`os`,`area`,
    `model`,`brand`,`sdk_version`,`gmail`,
    `height_width`,`app_time`,`network`,`lng`,`lat`,
    get_json_object(event_json,'$.kv.comment_id') AS comment_id,
    get_json_object(event_json,'$.kv.userid') AS userid,
    get_json_object(event_json,'$.kv.p_comment_id') AS p_comment_id,
    get_json_object(event_json,'$.kv.content') AS content,
    get_json_object(event_json,'$.kv.addtime') AS addtime,
    get_json_object(event_json,'$.kv.other_id') AS other_id,
    get_json_object(event_json,'$.kv.praise_count') AS praise_count,
    get_json_object(event_json,'$.kv.reply_count') AS reply_count,
    server_time
FROM gmall.dwd_event_log
WHERE dt='2020-06-22' AND event_name='comment';

-- 创建商品收藏表 dwd_favorites_log
DROP TABLE IF EXISTS dwd_favorites_log;
CREATE TABLE IF NOT EXISTS dwd_favorites_log(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '手机操作系统',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT '开发包版本',
    `gmail` string COMMENT '邮箱',
    `height_width` string COMMENT '屏幕高宽',
    `app_time` string COMMENT '客户端日志产生的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `id` string COMMENT '收藏ID',
    `course_id` string COMMENT '商品ID',
    `userid` string COMMENT '用户ID',
    `add_time` string COMMENT '创建时间',
    `server_time` string COMMENT '事件发生时间'
)
COMMENT '商品收藏表'
PARTITIONED BY(dt string);

-- 向商品收藏表 dwd_favorites_log 中填充数据
INSERT OVERWRITE TABLE dwd_favorites_log
PARTITION (dt='2020-06-22')
SELECT
    `mid_id`,`user_id`,`version_code`,`version_name`,
    `lang`,`source`,`os`,`area`,
    `model`,`brand`,`sdk_version`,`gmail`,
    `height_width`,`app_time`,`network`,`lng`,`lat`,
    get_json_object(event_json,'$.kv.id') AS id,
    get_json_object(event_json,'$.kv.course_id') AS course_id,
    get_json_object(event_json,'$.kv.userid') AS userid,
    get_json_object(event_json,'$.kv.add_time') AS add_time,
    server_time
FROM gmall.dwd_event_log
WHERE dt='2020-06-22' AND event_name='favorites';

-- 创建商品点赞表 dwd_praise_log
DROP TABLE IF EXISTS dwd_praise_log;
CREATE TABLE IF NOT EXISTS dwd_praise_log(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '手机操作系统',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT '开发包版本',
    `gmail` string COMMENT '邮箱',
    `height_width` string COMMENT '屏幕高宽',
    `app_time` string COMMENT '客户端日志产生的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `id` string COMMENT '点赞记录ID',
    `userid` string COMMENT '用户ID',
    `target_id` string COMMENT '点赞对象ID',
    `type` string COMMENT '点赞类型',
    `add_time` string COMMENT '添加事件',
    `server_time` string COMMENT '事件发生时间'
)
COMMENT '商品点赞表'
PARTITIONED BY(dt string);

-- 向商品点赞表 dwd_praise_log 中填充数据
INSERT OVERWRITE TABLE dwd_praise_log
PARTITION (dt='2020-06-22')
SELECT
    `mid_id`,`user_id`,`version_code`,`version_name`,
    `lang`,`source`,`os`,`area`,
    `model`,`brand`,`sdk_version`,`gmail`,
    `height_width`,`app_time`,`network`,`lng`,`lat`,
    get_json_object(event_json,'$.kv.id') AS id,
    get_json_object(event_json,'$.kv.userid') AS userid,
    get_json_object(event_json,'$.kv.target_id') AS target_id,
    get_json_object(event_json,'$.kv.type') AS type,
    get_json_object(event_json,'$.kv.add_time') AS add_time,
    server_time
FROM gmall.dwd_event_log
WHERE dt='2020-06-22' AND event_name='praise';

-- 创建错误日志表 dwd_error_log
DROP TABLE IF EXISTS dwd_error_log;
CREATE TABLE IF NOT EXISTS dwd_error_log(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '手机操作系统',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT '开发包版本',
    `gmail` string COMMENT '邮箱',
    `height_width` string COMMENT '屏幕高宽',
    `app_time` string COMMENT '客户端日志产生的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `errorBrief` string COMMENT '错误摘要',
    `errorDetail` string COMMENT '错误详情',
    `server_time` string COMMENT '事件发生时间'
)
COMMENT '错误日志表'
PARTITIONED BY(dt string);

-- 向错误日志表 dwd_error_log 中填充数据
INSERT OVERWRITE TABLE dwd_error_log
PARTITION (dt='2020-06-22')
SELECT
    `mid_id`,`user_id`,`version_code`,`version_name`,
    `lang`,`source`,`os`,`area`,
    `model`,`brand`,`sdk_version`,`gmail`,
    `height_width`,`app_time`,`network`,`lng`,`lat`,
    get_json_object(event_json,'$.kv.errorBrief') AS errorBrief,
    get_json_object(event_json,'$.kv.errorDetail') AS errorDetail,
    server_time
FROM gmall.dwd_event_log
WHERE dt='2020-06-22' AND event_name='error';



