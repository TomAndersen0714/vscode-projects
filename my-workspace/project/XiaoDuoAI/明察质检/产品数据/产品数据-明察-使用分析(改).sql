-- 产品数据-明察-XQC使用分析
-- 淘宝: clickhouse
WITH t1 AS (
    -- 统计XQC各客户店铺数
    SELECT
        company_id AS _id,
        count(1) AS shop_cnt
    FROM xqc_dim.xqc_shop_all
    WHERE `day` = toYYYYMMDD(yesterday())
    GROUP BY _id
),
t2 AS (
    -- 查询XQC各客户到期信息
    SELECT _id,
        name,
        shot_name,
        platforms,
        IF (
            dateDiff('day', create_date, expire_date) >= 40,
            '正式',
            '试用'
        ) AS customer_type,
        toDate(create_time) AS create_date,
        toDate(expire_time) AS expire_date,
        dateDiff('day', today(), expire_date) AS remain_days,
        dateDiff('day', create_date, expire_date) AS service_days
    FROM xqc_dim.company_all
    WHERE shot_name NOT IN (
            '何相玄',
            '测试',
            '客户端'
        )
        AND expire_date >= toDate(subtractWeeks(yesterday(), 1))
        AND `day` = toYYYYMMDD(yesterday())
),
t3 AS (
    -- 统计各客户页面访问情况
    SELECT
        arrayElement(splitByString(':', distinct_id), 1) AS shot_name,
        uniqExact(arrayElement(splitByString(':', distinct_id), 2)) AS uv,
        count(1) AS pv,
        uniqExact(`day`) AS active_days,
        min(`day`) AS min_day,
        max(`day`) AS max_day
    FROM ods.web_log_dis
    WHERE `day` BETWEEN toYYYYMMDD(toDate('{{ day.end=yesterday }}')) AND toYYYYMMDD(toDate('{{ day.start=month_ago }}'))
        -- 过滤页面查看动作
        AND `event` = '$pageview'
        -- 过滤明察质检url
        AND url LIKE '%xh-mc.xiaoduoai.com/%'
        -- 过滤单店和多店
        AND app_id IN ('xd001', 'xd023')
        AND shot_name GLOBAL IN (
            SELECT shot_name
            FROM xqc_dim.company_all
            WHERE day = toYYYYMMDD(yesterday())
        )
    GROUP BY shot_name
),
t4 AS (
    -- 查询客户开通版本信息
    SELECT user_name AS shot_name,
        arrayStringConcat(groupArray(role_name), ',') AS versions
    FROM dim.pri_center_version_all
    WHERE product_name = '明察质检(XQC)'
        AND user_id NOT in ('60f957f1c3d62bccb1606bd9', '方太', '测试')
    GROUP BY shot_name
)
t5 AS (
    -- 关联客户店铺数量和到期信息
    SELECT *
    FROM t2
    JOIN t1
    USING (_id)
),
t6 AS (
    -- 关联客户页面访问情况
    SELECT *
    FROM t5
    LEFT JOIN t3
    USING(shot_name)
),
-- 关联客户版本信息, 页面访问情况, 店铺数量, 到期信息, 并计算活跃率
SELECT *,
    round(
        active_days / 1.0000001 / (service_days - remain_days + 1),
        4
    ) as `active_ratio`
FROM t6
LEFT JOIN t4
USING shot_name