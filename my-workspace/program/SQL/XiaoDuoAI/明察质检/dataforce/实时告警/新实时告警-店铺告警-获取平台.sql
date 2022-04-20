-- 新实时告警-店铺告警-获取平台
SELECT
    plat_info.platform AS platform,
    CASE
        WHEN platform='tb' THEN '淘宝//tb'
        WHEN platform='jd' THEN '京东//jd'
        WHEN platform='ks' THEN '快手//ks'
        WHEN platform='dy' THEN '抖音//dy'
        WHEN platform='pdd' THEN '拼多多//pdd'
        WHEN platform='open' THEN '开放平台//open'
        ELSE platform
    END AS `平台`
FROM (
    SELECT arrayJoin(['tb','jd','ks','dy','pdd','open']) AS platform
) AS plat_info
WHERE platform GLOBAL IN (
    SELECT DISTINCT platform
    FROM xqc_dim.xqc_shop_all
    WHERE day = toYYYYMMDD(yesterday())
    AND company_id = '{{ company_id=5f73e9c1684bf70001413636 }}'
)