-- 产品数据-明察-XQC使用分析
-- 淘宝: clickhouse
WITH t1 AS (
    SELECT company_id AS _id,
        count(1) AS shop_cnt
    FROM xqc_dim.xqc_shop_all
    WHERE `day` = toYYYYMMDD(yesterday())
    GROUP BY _id
),
t2 AS (
    SELECT _id,
        name,
        shot_name,
        platforms,
        IF (
            dateDiff('day', created_time, expired_time) >= 40,
            '正式',
            '试用'
        ) AS customer_type,
        toDate(expired_time) AS expired_date,
        toDate(created_time) AS created_date,
        dateDiff('day', today(), expired_time) AS remain_days,
        dateDiff('day', created_time, expired_time) AS service_days
    FROM xqc_dim.company
    WHERE shot_name NOT IN (
            '何相玄',
            '测试',
            '客户端'
        )
        AND toDate(expired_time) >= toDate(subtractWeeks(yesterday(), 1))
),
t3 AS (
    SELECT arrayElement(splitByString(':', distinct_id), 1) AS shot_name,
        uniqExact(arrayElement(splitByString(':', distinct_id), 2)) AS uv,
        count(1) AS pv,
        uniqExact(`day`) AS active_days,
        min(`day`) AS min_day,
        max(`day`) AS max_day
    FROM ods.web_log_dis
    WHERE `day` <= toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
        AND `day` >= toYYYYMMDD(toDate('{{ day.start=month_ago }}'))
        AND `event` = '$pageview'
        AND url LIKE '%xh-mc.xiaoduoai.com/%'
        AND app_id IN ('xd001', 'xd023')
        AND shot_name GLOBAL IN (
            SELECT shot_name
            FROM xqc_dim.company
        )
    GROUP BY shot_name
),
t4 AS (
    SELECT *
    FROM t2
        JOIN t1 USING (_id)
),
t5 AS (
    SELECT *
    FROM t4
        LEFT JOIN t3 USING(shot_name)
),
t6 AS (
    SELECT user_name AS shot_name,
        arrayStringConcat(groupArray(role_name), ',') AS versions
    FROM dim.pri_center_version_all
    WHERE product_name = '明察质检(XQC)'
        AND user_id NOT in ('60f957f1c3d62bccb1606bd9', '方太', '测试')
    GROUP BY shot_name
)
SELECT *,
    round(
        active_days / 1.0000001 / (service_days - remain_days + 1),
        4
    ) as `active_ratio`
FROM t5
    left join t6 using shot_name