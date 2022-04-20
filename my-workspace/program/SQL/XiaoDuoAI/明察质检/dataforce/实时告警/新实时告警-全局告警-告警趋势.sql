-- 新实时告警-全局告警统计-告警趋势图
SELECT 
    day AS `日期`,
    CASE
        WHEN platform='jd' THEN '京东'
        WHEN platform='tb' THEN '淘宝'
        WHEN platform='ks' THEN '快手'
        WHEN platform='pdd' THEN '拼多多'
        WHEN platform='dy' THEN '抖音'
        WHEN platform='open' THEN '开放平台'
        ELSE platform
    END AS `平台`,
    platform_alert_daily_count -- 平台告警统计
FROM (
        SELECT * FROM (
            SELECT arrayJoin(
                arrayMap(
                    x->toYYYYMMDD(toDate(x)),
                    range(toUInt32(toDate('{{ day.start=week_ago }}')), toUInt32(toDate('{{ day.end=today }}') + 1), 1)
                )
            ) AS day
        ) AS time_axis
        GLOBAL CROSS JOIN (
            -- 已订阅店铺所在平台
            SELECT distinct platform
            FROM xqc_dim.company_tenant
            WHERE company_id = '{{ company_id=5f73e9c1684bf70001413636 }}'
        ) AS platform_axis
) AS time_platform_axis
GLOBAL LEFT JOIN (
    SELECT 
        day,
        platform,
        count(1) AS platform_alert_daily_count
    FROM xqc_ods.alert_all FINAL
    WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
        AND toYYYYMMDD(toDate('{{ day.end=today }}'))
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=5f73e9c1684bf70001413636 }}'
    )
    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{ shop_id_list=5bfe7a6a89bc4612f16586a5 }}') 
            OR
            snick IN splitByChar(',','{{ snick_list=null }}')
        )
    -- 下拉框筛选
    AND if({{ level=-1 }}!=-1,level={{ level=-1 }},level IN [1,2,3]) -- 告警等级
    AND if('{{ warning_type=全部 }}'!='全部',warning_type='{{ warning_type=全部 }}',warning_type!='') -- 告警内容
    GROUP BY day, platform
)
USING day,platform
ORDER BY day ASC, platform DESC