-- Q1:
/* 
第一步:查询表ods.web_log,将其中的create_time字段按照小时切分出来(SUBSTR()),新生成
create_time_hour字段,将distinct_id按照冒号进行分割(SPLIT_PART(distinct_id,':',1))
并获取第一个元素,即店铺主账号,新生成m_count字段.查询字段为distinct_id,create_time_hour,
m_count,生成表t1
PS: ClickHouse的With子句中只支持使用标量子查询或者字面量作为变量保存,而不能保存表查询过程,
即不能使用CTE
*/
SELECT
    distinct_id,
    substring(create_time,1,13) AS create_time_hour,
    splitByChar(':',distinct_id)[1] AS m_acount
FROM
    ods.web_log
/*
第二步:将表dim.practice_category作为t2,与t1进行LEFT JOIN,JOIN条件为
t1.main_count=t2.m_count,查询字段为t2.category_id,t1.create_time_hour,
t1.distinct_id生成表t3
*/
SELECT
    t2.category_id,
    t1.create_time_hour,
    t1.distinct_id
FROM (
    SELECT
        distinct_id,
        substring(create_time,1,13) AS create_time_hour,
        splitByChar(':',distinct_id)[1] AS m_acount
    FROM ods.web_log
) AS t1
LEFT JOIN dim.practice_category AS t2
ON t1.m_acount = t2.main_acount
/* 
第三步:查询表t3,按照category_id,create_time_hour字段进行分组,COUNT(distinct_id)作为PV
COUNT(DISTINCT distinct_id)作为UV
 */
SELECT
    category_id,
    create_time_hour,
    COUNT(distinct_id) AS PV,
    COUNT(DISTINCT distinct_id) AS UV
FROM (
    SELECT
        t2.category_id,
        t1.create_time_hour,
        t1.distinct_id
    FROM (
        SELECT
            distinct_id,
            substring(create_time,1,13) AS create_time_hour,
            splitByChar(':',distinct_id)[1] AS m_acount
        FROM ods.web_log
    ) AS t1
    LEFT JOIN dim.practice_category AS t2
    ON t1.m_acount = t2.main_acount
) AS t3
GROUP BY category_id,create_time_hour
ORDER BY category_id,create_time_hour

-- Q2:
/* 
第一步:查询ods.web_log表,过滤出event='$pageview'的记录,使用neighbor函数获取相同子账户(distinct_id)
相同天数(day),并按照create_time升序排序的下一个记录的create_time作为next_click_time
查询字段为distinct_id,day,create_time,next_click_time,被查询表为ods.web_log
 */
-- v1: 使用内建函数neighbor
-- PS: ClickHouse中的neighbor函数目前存在BUG,禁止使用
SELECT
    distinct_id,
    track_id,
    create_time,
    neighbor(create_time,1,NULL) AS next_click_time
FROM
    ods.web_log
WHERE
    event='$pageview'
-- v2: 使用相关子查询(关联子查询)
/* PS: ClickHouse中不支持关联子查询(相关子查询),此SQL失败*/
SELECT
    t1.distinct_id,
    `day`,
    t1.track_id,
    t1.create_time,
    (
        SELECT t2.create_time
        FROM ods.web_log AS t2
        RIGHT JOIN t1 
        ON t1.distinct_id = t2.distinct_id
        AND t1.day = t2.day
        AND t1.create_time < t2.create_time
        WHERE t2.event='$pageview'
        ORDER BY t2.create_time
        LIMIT 1
    ) AS next_click_time
FROM
    ods.web_log AS t1
WHERE
    event='$pageview'
-- v3:使用ArrayJoin实现OVER(PARTITION BY distinct_id,day ORDER BY create_time)功能
-- 第1.1步:生成表t1
SELECT
    distinct_id,
    `day`,
    track_id,
    create_time,
    row_number
FROM (
    SELECT
        distinct_id,
        `day`,
        groupArray(track_id) AS track_id_arr,
        groupArray(create_time) AS time_arr,
        arrayEnumerate(time_arr) AS row_number_arr
    FROM (
        SELECT *
        FROM ods.web_log
        WHERE event='$pageview'
        ORDER BY create_time
    )
    GROUP BY distinct_id,`day`
)
ARRAY JOIN
    track_id_arr AS track_id,
    time_arr AS create_time,
    row_number_arr AS row_number
ORDER BY distinct_id,`day`,create_time
-- 第1.2步:基于表t1,进行自连接查询,两张相同的表分别命名为t1和t2,LEFT JOIN条件为
-- t1.distint_id = t2.distinct_id AND t1.day = t2.day AND
-- t1.row_number + 1 = toUInt64(t2.row_number)
-- 生成的结果表视为t3
-- PS:row_number为UInt64类型,对其进行比较时,需要使用toUInt进行转换
SELECT
    t1.distinct_id,
    t1.`day`,
    t1.track_id,
    t1.create_time,
    t2.create_time AS next_click_time
FROM t1
LEFT JOIN t1 AS t2
ON t1.distinct_id = t2.distinct_id
AND t1.day = t2.day
AND t1.row_number + 1 = toUInt64(t2.row_number)

/* 
第二步:查询ods.web_log表,过滤出event='$WebClick'的记录,生成表t4
 */
SELECT
    distinct_id,
    `day`,
    create_time
FROM ods.web_log
WHERE event='$WebClick'

/* 
第三步:连接查询t3和t4,Join条件为t3.distinct_id = t4.distinct_id AND
t3.day = t4.day AND t3.create_time < t4.create_time AND
(t3.next_click_time IS NULL OR t4.create_time < t3.next_click_time)
 */
-- PS:clickhouse中的JOIN子句的ON子句中,必须要求表达式左右为两张表的字段
-- 中间使用数学比较符号进行连接,不只支持表达式中只使用单个表的某个字段进行
-- 过滤.因此当前
-- v1:此版本不可用,因为JOIN ON连接条件只支持两个表的字段的比较式作为条件

-- 整合
SELECT
    t3.track_id,
    COUNT(*) AS click_count
FROM t3
LEFT JOIN t4
ON t3.distinct_id = t4.distinct_id
AND t3.`day` = t4.`day`
AND (t3.next_click_time IS NULL OR t4.create_time < t3.next_click_time)
GROUP BY track_id
ORDER BY click_count DESC
LIMIT 50
-- v2:使用Where代替JOIN进行连接
SELECT
    t3.track_id,
    COUNT(*) AS click_count
FROM (
    SELECT
        t1.distinct_id,
        t1.`day`,
        t1.track_id,
        t1.create_time,
        t2.create_time AS next_click_time
    FROM (
        SELECT
            distinct_id,
            `day`,
            track_id,
            create_time,
            row_number
        FROM (
            SELECT
                distinct_id,
                `day`,
                groupArray(track_id) AS track_id_arr,
                groupArray(create_time) AS time_arr,
                arrayEnumerate(time_arr) AS row_number_arr
            FROM (
                SELECT *
                FROM ods.web_log
                WHERE event='$pageview'
                ORDER BY create_time
            ) AS t0
            GROUP BY distinct_id,`day`
        )
        ARRAY JOIN
            track_id_arr AS track_id,
            time_arr AS create_time,
            row_number_arr AS row_number
        ORDER BY distinct_id,`day`,create_time
    ) AS t1
    LEFT JOIN (
        SELECT
            distinct_id,
            `day`,
            track_id,
            create_time,
            row_number
        FROM (
            SELECT
                distinct_id,
                `day`,
                groupArray(track_id) AS track_id_arr,
                groupArray(create_time) AS time_arr,
                arrayEnumerate(time_arr) AS row_number_arr
            FROM (
                SELECT *
                FROM ods.web_log
                WHERE event='$pageview'
                ORDER BY create_time
            )
            GROUP BY distinct_id,`day`
        )
        ARRAY JOIN
            track_id_arr AS track_id,
            time_arr AS create_time,
            row_number_arr AS row_number
        ORDER BY distinct_id,`day`,create_time
    ) AS t2
    ON t1.distinct_id = t2.distinct_id
    AND t1.day = t2.day
    AND t1.row_number + 1 = toUInt64(t2.row_number)
) AS t3
LEFT JOIN (
    SELECT
        distinct_id,
        `day`,
        create_time
    FROM ods.web_log
    WHERE event='$WebClick'
) AS t4
ON t3.distinct_id = t4.distinct_id
AND t3.`day` = t4.`day`
WHERE 
    t3.next_click_time IS NULL 
    OR
    t4.create_time < t3.next_click_time
GROUP BY track_id
ORDER BY click_count DESC
LIMIT 50


SELECT
    t3.track_id,
    COUNT(*) AS click_count
FROM (
    SELECT
        t1.distinct_id,
        t1.`day`,
        t1.track_id,
        t1.create_time,
        t2.create_time AS next_click_time
    FROM (
        SELECT
            distinct_id,
            `day`,
            track_id,
            create_time,
            row_number
        FROM (
            SELECT
                distinct_id,
                `day`,
                groupArray(track_id) AS track_id_arr,
                groupArray(create_time) AS time_arr,
                arrayEnumerate(time_arr) AS row_number_arr
            FROM (
                SELECT *
                FROM ods.web_log
                WHERE event='$pageview'
                ORDER BY create_time
            ) AS t1_1
            GROUP BY distinct_id,`day`
        )AS t1_2
        ARRAY JOIN
            track_id_arr AS track_id,
            time_arr AS create_time,
            row_number_arr AS row_number
        ORDER BY distinct_id,`day`,create_time
    ) AS t1
    LEFT JOIN (
        SELECT
            distinct_id,
            `day`,
            track_id,
            create_time,
            row_number
        FROM (
            SELECT
                distinct_id,
                `day`,
                groupArray(track_id) AS track_id_arr,
                groupArray(create_time) AS time_arr,
                arrayEnumerate(time_arr) AS row_number_arr
            FROM (
                SELECT *
                FROM ods.web_log
                WHERE event='$pageview'
                ORDER BY create_time
            ) t2_1
            GROUP BY distinct_id,`day`
        ) AS t2_2
        ARRAY JOIN
            track_id_arr AS track_id,
            time_arr AS create_time,
            row_number_arr AS row_number
        ORDER BY distinct_id,`day`,create_time
    ) AS t2
    ON t1.distinct_id = t2.distinct_id
    AND t1.day = t2.day
    AND t1.row_number + 1 = toUInt64(t2.row_number)
) AS t3
LEFT JOIN (
    SELECT
        distinct_id,
        `day`,
        create_time
    FROM ods.web_log
    WHERE event='$WebClick'
) AS t4
ON t3.distinct_id = t4.distinct_id
AND t3.`day` = t4.`day`
WHERE 
    t3.next_click_time IS NULL 
    OR
    t4.create_time < t3.next_click_time
GROUP BY track_id
ORDER BY click_count DESC
LIMIT 50


-- Q3:
-- 第一步:查询 ods.web_log 表,将create_time从String类型转换成DateTime类型,并向下取整到
-- 小时,作为新字段create_time_hour,查询字段为 distinct_id,create_time_hour.
-- 查询结果作为表t1
SELECT DISTINCT
    distinct_id,
    toStartOfHour(toDateTime(create_time)) AS create_time_hour
FROM ods.web_log
ORDER BY create_time_hour
-- 第二步:基于表t1,按照distinct_id进行分组,使用groupArray(create_time_hour) AS create_time_arr和
-- arrayEnumerate(create_time_arr)获取每个distinct_id中create_time_hour的行号,作为row_number_arr字段
-- 查询结果作为表t2
SELECT
    distinct_id,
    groupArray(create_time_hour) AS create_time_arr,
    arrayEnumerate(create_time_arr) AS row_number_arr
FROM (
    SELECT DISTINCT
        distinct_id,
        toStartOfHour(toDateTime(create_time)) AS create_time_hour
    FROM ods.web_log
    ORDER BY create_time_hour ASC
) AS t1
GROUP BY distinct_id
-- 第三步:基于表t2进行Array Join,将create_time_arr和row_number_arr拆分成单个元素
-- 与当前行的其他列进行JOIN,同时计算create_time_hour与row_number的差值
-- 此查询结果作为表t3
SELECT
    distinct_id,
    create_time_hour,
    row_number,
    addHours(create_time_hour,-row_number) AS diff
FROM t2
ARRAY JOIN 
    create_time_arr AS create_time_hour,
    row_number_arr AS row_number
ORDER BY
    distinct_id,row_number
-- 第四步:查询表t3,按照distinct_id,diff分组,然后统计每个组内的记录个数AS continues_hours
-- 查询结果作为t4
SELECT
    distinct_id,
    COUNT(*) AS continues_hours
FROM t3
GROUP BY distinct_id,diff
-- 第五步:查询表t4,按照distinct_id进行分组,选取max(continues_hours)作为max_continues_hours
SELECT
    distinct_id,
    max(continues_hours) AS max_continues_hours
FROM t4
GROUP BY distinct_id
-- 整合
SELECT
    distinct_id,
    max(continues_hours) AS max_continues_hours
FROM (
    SELECT
        distinct_id,
        COUNT(*) AS continues_hours
    FROM (
        SELECT
            distinct_id,
            create_time_hour,
            row_number,
            addHours(create_time_hour,-row_number) AS diff
        FROM (
            SELECT
                distinct_id,
                groupArray(create_time_hour) AS create_time_arr,
                arrayEnumerate(create_time_arr) AS row_number_arr
            FROM (
                SELECT DISTINCT
                    distinct_id,
                    toStartOfHour(toDateTime(create_time)) AS create_time_hour
                FROM ods.web_log
                ORDER BY create_time_hour ASC
            ) AS t1
            GROUP BY distinct_id
        ) AS t2
        ARRAY JOIN
            create_time_arr AS create_time_hour,
            row_number_arr AS row_number
        ORDER BY
            distinct_id,row_number
    ) AS t3
    GROUP BY distinct_id,diff
) AS t4
GROUP BY distinct_id



