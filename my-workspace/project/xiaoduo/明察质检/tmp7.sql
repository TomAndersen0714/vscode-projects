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
            '测试企业2',
            '测试企业1',
            '企业平台的测试企业',
            '测试账号汇总',
            '晓客科技测试',
            'cvd',
            '智晓多谋',
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
t7 AS (
    SELECT shot_name,
        (
            CASE
                WHEN `version` IN ('1', '3', '4', '8', '6') THEN '企业版'
                WHEN `version` IN ('0') THEN '商户版'
                WHEN `version` IN ('2') THEN '通用版'
                WHEN `version` IN ('5', '7', '') THEN '测试版'
                ELSE `version`
            END
        ) AS `version`,
        id
    FROM xqc_dim.version_all
    where `day` = toYYYYMMDD(yesterday())
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
                ------------------------企业版统计------------------------------
                WHEN version IN ('企业版') AND pv >= 2474 AND (uv >= 15 OR active_days >= 29) THEN '高活'
                WHEN version IN ('企业版') AND pv >= 180 AND(uv >= 1 OR active_days >= 12) THEN '正常'
                WHEN version IN ('企业版')  AND ((0 < pv AND pv< 180) OR (0 < uv AND uv < 1)  OR (0 < active_days AND active_days < 12) ) THEN '低活'
                WHEN version IN ('企业版') AND pv = 0 AND uv = 0 AND active_days = 0 THEN '未启用'
                ------------------------商户版统计------------------------------
                WHEN version IN ('商户版') AND pv >= 236 AND ( uv >= 1 OR active_days >= 20) THEN '高活'
                WHEN version IN ('商户版') AND pv >= 22 AND ( uv >= 1 OR active_days >= 3 ) THEN '正常'
                WHEN version IN ('商户版') AND ( (0 < pv AND pv< 22) OR (0 < uv AND uv < 1)  OR (0 < active_days AND active_days < 3)  ) THEN '低活'
                WHEN version IN ('商户版') AND ( pv = 0 AND uv = 0 AND active_days = 0 ) THEN '未启用'
                ------------------------通用版统计------------------------------
                WHEN version IN ('通用版') AND pv >= 140 AND ( uv >= 1 OR active_days >= 18 ) THEN '高活'
                WHEN version IN ('通用版') AND pv >= 27 AND ( uv >= 1 OR active_days >= 2 ) THEN '正常'
                WHEN version IN ('通用版') AND ( (0 < pv AND pv< 27) OR (0 < uv AND uv < 1)  OR (0 < active_days AND active_days < 2)  ) THEN '低活'
                WHEN version IN ('通用版') AND ( pv = 0 AND uv = 0 AND active_days = 0 ) THEN '未启用'
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
WHERE shot_name NOT IN (
        SELECT shot_name
        FROM mayfly.xqc_corp_desc
    )