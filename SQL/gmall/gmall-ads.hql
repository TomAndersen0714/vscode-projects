-- ADS层建表语句
-- 使用Tez引擎处理所有的查询语句
set hive.execution.engine=tez;

-- 创建活跃用户统计表 ads_uv_count
DROP TABLE IF EXISTS gmall.ads_uv_count;
CREATE TABLE IF NOT EXISTS gmall.ads_uv_count(
    `dt` string COMMENT '统计日期',
    `day_count` bigint COMMENT '当天活跃用户数',
    `wk_count` bigint COMMENT '当周活跃用户数',
    `mn_count` bigint COMMENT '当月活跃用户数',
    `is_weekend` string COMMENT '(Y/N)是否是周末,用于统计整周数据',
    `is_monthend` string COMMENT '(Y/N)是否是月末,用于统计整月数据'
) COMMENT '活跃用户统计表'
ROW FORMAT
DELIMITED FIELDS TERMINATED BY '\t';


-- 向活跃用户统计表 ads_uv_count 中填充数据
-- 第一步:统计日活用户数
SELECT count(1) AS count
FROM gmall.dws_uv_detail_day
WHERE dt='2020-06-12';
-- 第二步:统计周活用户数
SELECT count(1) AS count
FROM gmall.dws_uv_detail_wk
WHERE wk_dt=concat(
    date_add(next_day('2020-06-12','MONDAY'),-7),'-',
    date_add(next_day('2020-06-12','MONDAY'),-1));
-- 第三步:统计月活用户数
SELECT count(1) AS count
FROM gmall.dws_uv_detail_mn
WHERE mn=date_format('2020-06-12','yyyy-MM');
-- 第四部:汇总各个周期日活用户数
-- PS:由于Hive不支持SELECT子句,只支持FROM子句查询,WHERE子句查询
-- 因此需要使用连接查询将查询结果表连接,然后统计.
-- 由于查询结果都是一行,因此此处直接使用笛卡尔积.
set hive.strict.checks.cartesian.product=false;
INSERT INTO TABLE gmall.ads_uv_count
SELECT
    '2020-06-12' AS dt,
    tbl_1.count AS day_count,
    tbl_2.count AS wk_count,
    tbl_3.count AS mn_count,
    if(date_add(next_day('2020-06-12','MONDAY'),-1)='2020-06-12','Y','N') AS is_weekend,
    if(last_day('2020-06-12')='2020-06-12','Y','N') AS is_monthend
FROM
(
    SELECT count(1) AS count
    FROM gmall.dws_uv_detail_day
    WHERE dt='2020-06-12'
)AS tbl_1,
(
    SELECT count(1) AS count
    FROM gmall.dws_uv_detail_wk
    WHERE wk_dt=concat(
        date_add(next_day('2020-06-12','MONDAY'),-7),'-',
        date_add(next_day('2020-06-12','MONDAY'),-1))
)AS tbl_2,
(
    SELECT count(1) AS count
    FROM gmall.dws_uv_detail_mn
    WHERE mn=date_format('2020-06-12','yyyy-MM')
)AS tbl_3;


-- 创建每日新增设备统计表 ads_new_mid_day_count
DROP TABLE IF EXISTS gmall.ads_new_mid_day_count;
CREATE TABLE IF NOT EXISTS gmall.ads_new_mid_day_count(
    `count_date` string COMMENT '统计日期',
    `new_mid_count` bigint COMMENT '新增设备数量'
) COMMENT '每日新增设备数量统计表'
ROW FORMAT
DELIMITED FIELDS TERMINATED BY '\t';


-- 向每日新增设备统计表 ads_new_mid_day_count 中填充数据(每天统计一次)
-- DELETE FROM gmall.ads_new_mid_day_count WHERE dt='2020-06-12';
INSERT INTO TABLE gmall.ads_new_mid_day_count
SELECT
    '2020-06-12',
    count(*)
FROM gmall.dws_new_mid_day
WHERE dt='2020-06-12';

-- 创建每周新增设备统计表 ads_new_mid_week_count
DROP TABLE IF EXISTS gmall.ads_new_mid_week_count;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ads_new_mid_week_count(
    `count_week` string COMMENT '统计日期',
    `new_mid_count` bigint COMMENT '新增设备数量'
) COMMENT '每周新增设备数量统计表'
ROW FORMAT
DELIMITED FIELDS TERMINATED BY '\t';

-- 向每周新增设备统计表 ads_new_mid_day_count 中填充数据(每周统计一次)
INSERT INTO TABLE gmall.ads_new_mid_week_count
SELECT
    concat(date_add(next_day('2020-06-12','MONDAY'),-7),
        '-',date_add(next_day('2020-06-12','MONDAY'),-1)),
    count(*)
FROM gmall.dws_new_mid_wk
WHERE week=concat(
    date_add(next_day('2020-06-12','MONDAY'),-7),'-',
    date_add(next_day('2020-06-12','MONDAY'),-1))
GROUP BY week;

-- 创建每月新增设备统计表 ads_new_mid_month_count
DROP TABLE IF EXISTS gmall.ads_new_mid_month_count;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ads_new_mid_month_count(
    `count_month` string COMMENT '统计日期',
    `new_mid_count` bigint COMMENT '新增设备数量'
) COMMENT '每月新增设备数量统计表'
ROW FORMAT
DELIMITED FIELDS TERMINATED BY '\t';

-- 向每月新增设备统计表 ads_new_mid_month_count 中填充数据
INSERT INTO TABLE gmall.ads_new_mid_month_count
SELECT
    date_format('2020-06-12','yyyy-MM'),
    count(*)
FROM gmall.dws_new_mid_mn
WHERE month=date_format('2020-06-12','yyyy-MM');