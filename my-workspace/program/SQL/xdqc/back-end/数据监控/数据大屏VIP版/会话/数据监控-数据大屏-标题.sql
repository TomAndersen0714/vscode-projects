SELECT
    CONCAT(
        shot_name,
        '实时质检数组大屏 - ',
        CASE
            WHEN '{{platform}}' = 'tb' THEN '天猫平台'
            WHEN '{{platform}}' = 'jd' THEN '京东平台'
            WHEN '{{platform}}' = 'ks' THEN '快手平台'
            ELSE '其他平台'
        END
    ) AS title
FROM ods.xinghuan_company_all
WHERE day = toYYYYMMDD(yesterday())
AND _id = '{{company_id=61602afd297bb79b69c06118}}'
LIMIT 1