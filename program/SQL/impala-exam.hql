-- Q1:
-- Impala不支持Analytic Function中使用DISTINCT,可以使用WITH clause代替
-- 'main_acount'指的是晓多客服主账号,一个主账号可以有多个子账号,web_log记录的是
-- 这些客服账号访问晓多后台管理网站的记录.'distinct_id'认为是子账号名.
-- 'category_id'认为是主账号/店铺类型
SELECT
    t3.category_id,
    t3.create_time_hour,
    COUNT(distinct_id) AS PV,
    COUNT(DISTINCT distinct_id) AS UV
FROM
(
    WITH t1 AS (
        SELECT
            distinct_id,
            SUBSTR(create_time,1,13) AS create_time_hour,
            SPLIT_PART(distinct_id,':',1) AS m_count
        FROM ods.web_log
    )
    SELECT t1.distinct_id,t1.create_time_hour,t1.m_count,t2.category_id
    FROM t1
    LEFT JOIN dim.practice_category AS t2
    ON t2.main_acount = t1.m_count
    
) AS t3
GROUP BY category_id,create_time_hour
ORDER BY category_id,create_time_hour

-- Q2:
-- 第一步:过滤出event='$pageview'的log,并使用窗口函数LAG查询当前distinct_id,当天的
-- 下一次click出现的时间AS next_create_time
-- 如果下一次click的create_time和前一次click的create_time不是同一天,即day字段值不相同
-- 则将下一跳click的时间设置为隔天凌晨0点
-- 生成表t1,查询字段为distinct_id,track_id,create_time,next_create_time
-- DATE_ADD(TO_TIMESTAMP(SUBSTR(create_time,1,10),'yyyy-MM-dd'),1)
SELECT
    distinct_id,
    track_id,
    create_time,
    LAG(create_time,1) over(partition by distinct_id,day order by create_time ) as next_create_time
FROM ods.web_log
WHERE event='$pageview'
-- 第二步:查询原表,过滤出event='$WebClick'的log生成表t2
SELECT
    distinct_id,
    create_time
FROM ods.web_log
WHERE event='$WebClick'
-- 第三步:将t2与t1进行Join查询,Join条件为t1.distinct_id = t2.distinct_id AND
-- t1.day = t2.day AND t2.create_time >= t1.create_time 
-- AND (t1.next_click_time IS NULL OR t2.create_time < t1.next_click_time)
-- 然后按照track_id分组,统计每个PV组中记录个数COUNT(*) AS click_count
WITH t1 AS (
    SELECT
        distinct_id,
        track_id,
        create_time,
        day,
        LAG(create_time,1) over(partition by distinct_id,day order by create_time ) as next_click_time
    FROM ods.web_log
    WHERE event='$pageview'
),t2 AS (
    SELECT
        distinct_id,
        create_time,
        day
    FROM ods.web_log
    WHERE event='$WebClick'
)
SELECT
    t1.track_id,
    COUNT(*) AS click_count
FROM t1
LEFT JOIN t2
ON t1.distinct_id = t2.distinct_id
    AND t1.day = t2.day
    AND t2.create_time > t1.create_time
    AND (t1.next_click_time IS NULL OR t2.create_time < t1.next_click_time)
GROUP BY track_id
ORDER BY click_count DESC
LIMIT 50


-- Q3:
-- 第一步:查询表ods.web_log,将string类型的create_time转换成TIMESTAMP类型(TO_TIMESTAMP),并按照小时进行截断(DATE_TRUNC),生成表t1
-- 查询字段为,distinct_id,create_time_hour
SELECT DISTINCT
    distinct_id,
    DATE_TRUNC('hour',TO_TIMESTAMP(create_time,'yyyy-MM-dd HH:mm:ss')) AS create_time_hour
FROM ods.web_log

-- 第二步:查询表t1,使用开窗函数(ROW_NUMBER()),获取每条PV记录在其distinct_id组中的排名,排名按照create_time_hour升序排序
-- 并计算create_time_hour与排名的差值AS diff,查询字段为distinct_id,create_time_hour,diff,生成表t2
WITH t1 AS (
    SELECT DISTINCT
        distinct_id,
        DATE_TRUNC('hour',TO_TIMESTAMP(create_time,'yyyy-MM-dd HH:mm:ss')) AS create_time_hour
    FROM ods.web_log
)
SELECT
    distinct_id,
    create_time_hour,
    HOURS_SUB(
        create_time_hour,
        ROW_NUMBER() OVER(PARTITION BY distinct_id ORDER BY create_time_hour)
        )  AS diff
FROM t1
-- 第三步:查询表t2,将记录按照distinct_id,diff进行分组,选出count值AS continuous_hours,生成表t3
WITH t1 AS (
    SELECT DISTINCT
        distinct_id,
        DATE_TRUNC('hour',TO_TIMESTAMP(create_time,'yyyy-MM-dd HH:mm:ss')) AS create_time_hour
    FROM ods.web_log
), t2 AS (
    SELECT
        distinct_id,
        create_time_hour,
        HOURS_SUB(
            create_time_hour,
            ROW_NUMBER() OVER(PARTITION BY distinct_id ORDER BY create_time_hour)
            )  AS diff
    FROM t1
)
SELECT
    distinct_id,
    COUNT(*) AS continuous_hours_count
FROM t2
GROUP BY distinct_id,diff
-- 第四步:查询表t3,将记录按照distinct_id分组,选出MAX(continuous_hours),生成表t4
WITH t1 AS (
    SELECT DISTINCT
        distinct_id,
        DATE_TRUNC('hour',TO_TIMESTAMP(create_time,'yyyy-MM-dd HH:mm:ss')) AS create_time_hour
    FROM ods.web_log
), t2 AS (
    SELECT
        distinct_id,
        create_time_hour,
        HOURS_SUB(
            create_time_hour,
            ROW_NUMBER() OVER(PARTITION BY distinct_id ORDER BY create_time_hour)
            )  AS diff
    FROM t1
), t3 AS (
    SELECT
        distinct_id,
        COUNT(*) AS continuous_hours_count
    FROM t2
    GROUP BY distinct_id,diff
)
SELECT distinct_id,MAX(continuous_hours_count) AS max_hours
FROM t3
GROUP BY distinct_id
ORDER BY max_hours DESC