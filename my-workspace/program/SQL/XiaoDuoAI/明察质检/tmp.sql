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
    WHERE `day` <= toYYYYMMDD(toDate('{{ 结束日期 }}'))
        AND `day` >= toYYYYMMDD(toDate('{{ 开始日期 }}'))
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
        AND user_id NOT IN (
            '60f957f1c3d62bccb1606bd9',
            '方太',
            '晓多',
            '测试'
        )
    GROUP BY shot_name
),
t7 AS (
    SELECT shot_name,
        (
            CASE
                WHEN versions IN (
                    '企业版+大屏',
                    '企业版-宝尊'
                ) THEN '企业版'
                ELSE versions
            END
        ) AS versions
    FROM t6
    WHERE versions NOT IN ('测试', '晓多')
),
t8 AS (
    SELECT shot_name AS customer_name,
        *,
        (service_days - remain_days + 1) AS consumimg_days,
        round(
            active_days / 1.0000001 / (service_days - remain_days + 1),
            4
        ) AS `active_ratio`
    FROM t5
        LEFT JOIN t7 USING shot_name
),
t9 AS (
    SELECT *,
        (
            CASE
                WHEN consumimg_days >= 60 THEN '维护期'
                ELSE '交付期'
            END
        ) AS service_stage,
        (
            CASE
                WHEN versions IN ('企业版')
                AND (
                    uv >= 5
                    AND pv >= 500
                    AND active_days >= 15
                ) THEN '高活'
                WHEN versions IN ('企业版')
                AND (
                    uv >= 5
                    OR pv >= 500
                    OR active_days >= 15
                ) THEN '正常'
                WHEN versions IN ('企业版')
                AND (
                    uv < 5
                    AND pv < 500
                    AND active_days < 15
                ) THEN '低活'
                WHEN versions IN ('商户版')
                AND (
                    uv >= 2
                    AND pv >= 300
                    AND active_days >= 5
                ) THEN '高活'
                WHEN versions IN ('商户版')
                AND (
                    uv >= 1
                    OR pv >= 200
                    OR active_days >= 5
                ) THEN '正常'
                WHEN versions IN ('商户版')
                AND (
                    uv < 1
                    AND pv < 100
                    AND active_days < 3
                ) THEN '低活'
                WHEN versions IN ('通用版')
                AND (
                    uv >= 2
                    AND pv >= 300
                    AND active_days >= 5
                ) THEN '高活'
                ELSE '低活'
            END
        ) AS is_active
    FROM t8
    WHERE customer_name NOT IN ('晓多')
),
tx AS (
    SELECT name,
        tonnage_level,
        sum(payment) AS payment
    FROM (
            SELECT DISTINCT customer_name AS name,
                contract_type,
                tonnage_level,
                toInt32(webapp_value) AS payment,
                day_end
            FROM dws.crm_shop_contract_all
            WHERE is_had_webapp = '是'
                AND toDate(day_end) >= toDate(today())
        )
    GROUP BY name,
        tonnage_level
)
SELECT *
FROM t9
    LEFT JOIN tx USING name
where customer_name like '%{{ customer_name }}%'
    AND versions like '%{{ 版本 }}%'
    AND is_active like '%{{ 活跃等级 }}%'
    AND service_stage like '%{{ 服务阶段 }}%'