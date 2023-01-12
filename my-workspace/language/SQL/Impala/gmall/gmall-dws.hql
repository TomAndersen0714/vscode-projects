-- DWS层建表语句

-- 使用Tez引擎处理所有的查询语句
set hive.execution.engine=tez;
-- 允许在不指定静态分区的情况下使用动态分区功能
set hive.exec.dynamic.partition.mode=nonstrict;
-- 创建日活用户明细表 gmall.dws_uv_detail_day
DROP TABLE IF EXISTS gmall.dws_uv_detail_day;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall.dws_uv_detail_day(
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
    `lat` string COMMENT '纬度'
) COMMENT '日活用户分析表'
PARTITIONED BY(dt string);

-- 向日活用户明细表 gmall.dws_uv_detail_day 中填充数据
INSERT OVERWRITE TABLE gmall.dws_uv_detail_day
PARTITION(dt='2020-06-12')
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
FROM gmall.dwd_start_log
WHERE dt='2020-06-12'
GROUP BY mid_id;

-- 统计指定日期的日活用户数
SELECT count(*) FROM gmall.dws_uv_detail_day;


-- 创建周活用户明细表 gmall.dws_uv_detail_wk
DROP TABLE IF EXISTS gmall.dws_uv_detail_wk;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall.dws_uv_detail_wk(
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
    `monday_date` string COMMENT '周一日期',
    `sunday_date` string COMMENT '周日日期'
) COMMENT '周活用户明细表'
PARTITIONED BY(week string);

-- 向周活用户明细表 gmall.dws_uv_detail_wk 中填充数据(基于日活表统计周活用户)
INSERT OVERWRITE TABLE gmall.dws_uv_detail_wk
-- 使用动态分区功能,根据查询结果最后一列来确定所在分区
PARTITION(week) 
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
    date_add(next_day('2020-06-12','MONDAY'),-7) AS monday_date,
    date_add(next_day('2020-06-12','MONDAY'),-1) AS sunday_date,
    concat(
        date_add(next_day('2020-06-12','MONDAY'),-7),'-',
        date_add(next_day('2020-06-12','MONDAY'),-1)) AS week
FROM gmall.dws_uv_detail_day
WHERE 
    dt>=date_add(next_day('2020-06-12','MONDAY'),-7)
AND 
    dt<=date_add(next_day('2020-06-12','MONDAY'),-1)
GROUP BY mid_id;

-- 查询并验证导入结果
SELECT * FROM gmall.dws_uv_detail_wk LIMIT 1;
SELECT count(*) FROM gmall.dws_uv_detail_wk;


-- 创建月活用户明细表 gmall.dws_uv_detail_mn
DROP TABLE IF EXISTS gmall.dws_uv_detail_mn;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall.dws_uv_detail_mn(
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
    `lat` string COMMENT '纬度'
) COMMENT '月活用户明细表'
PARTITIONED BY (month string);

-- 向月活用户明细表 gmall.dws_uv_detail_mn 中填充数据(基于日活表统计月活用户)
INSERT OVERWRITE TABLE gmall.dws_uv_detail_mn
-- 使用动态分区功能,根据查询结果最后一列来确定所在分区
PARTITION(month)
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
    date_format('2020-06-12','yyyy-MM') AS month
FROM gmall.dws_uv_detail_day
WHERE date_format(dt,'yyyy-MM')=date_format('2020-06-12','yyyy-MM')
GROUP BY mid_id;

-- 查询并验证结果
SELECT * FROM gmall.dws_uv_detail_mn LIMIT 1;


-- 创建每日新增设备信息表 dws_new_mid_day
DROP TABLE IF EXISTS gmall.dws_new_mid_day;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall.dws_new_mid_day(
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
    `lat` string COMMENT '纬度'
) COMMENT '每日新增设备信息表'
PARTITIONED BY(dt string);

-- 向每日新增设备信息表 dws_new_mid_day 中填充数据(基于表 gmall.dws_uv_detail_day)
INSERT OVERWRITE TABLE gmall.dws_new_mid_day
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
    '2020-06-12' AS dt
FROM gmall.dws_uv_detail_day AS uv_day
LEFT JOIN (
    SELECT *
    FROM gmall.dws_new_mid_day
    WHERE dt<'2020-06-12'
) AS nm_day
ON uv_day.mid_id = nm_day.mid_id
WHERE uv_day.dt='2020-06-12'
AND nm_day.mid_id IS NULL;


-- 创建每周新增设备信息表 dws_new_mid_wk
DROP TABLE IF EXISTS gmall.dws_new_mid_wk;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall.dws_new_mid_wk(
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
    `monday_date` string COMMENT '周一日期',
    `sunday_date` string COMMENT '周日日期'
) COMMENT '每周新增设备信息表'
PARTITIONED BY(week string);

-- 向每周新增设备信息表 dws_new_mid_wk 填充数据(基于表 gmall.dws_uv_detail_wk)
INSERT OVERWRITE TABLE gmall.dws_new_mid_wk
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
FROM gmall.dws_uv_detail_wk AS uv_wk
LEFT JOIN (
    SELECT *
    FROM gmall.dws_new_mid_wk
    WHERE sunday_date<'2020-06-12'
) AS nm_wk
ON uv_wk.mid_id = nm_wk.mid_id
WHERE uv_wk.week=concat(date_add(next_day('2020-06-12','MONDAY'),-7),
    '-',date_add(next_day('2020-06-12','MONDAY'),-1))
AND nm_wk.mid_id IS NULL;


-- 创建每月新增设备信息表 dws_new_mid_mn
DROP TABLE IF EXISTS gmall.dws_new_mid_mn;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall.dws_new_mid_mn(
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
    `lat` string COMMENT '纬度'
)
PARTITIONED BY(month string);

-- 向每月新增设备信息表 dws_new_mid_mn 中填充数据(基于表 gmall.dws_uv_detail_mn)
INSERT OVERWRITE TABLE gmall.dws_new_mid_mn
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
FROM gmall.dws_uv_detail_mn AS uv_mn
LEFT JOIN (
    SELECT *
    FROM gmall.dws_new_mid_mn
    WHERE month<date_format('2020-06-12','yyyy-MM')
) AS nm_mn
ON uv_mn.mid_id = nm_mn.mid_id
WHERE uv_mn.month = date_format('2020-06-12','yyyy-MM');