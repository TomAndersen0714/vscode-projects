-- 新实时告警-平台告警-趋势图
SELECT
    day,
    seller_nick,
    cnt
FROM (
    SELECT day,seller_nick
    FROM (
        SELECT arrayJoin(
            arrayMap(
                x->toYYYYMMDD(toDate(x)),
                range(toUInt32(toDate('{{ day.start=week_ago }}')), toUInt32(toDate('{{ day.end=today }}') + 1), 1)
            )
        ) AS day
    ) AS time_axis
    GLOBAL CROSS JOIN (
        SELECT DISTINCT
            seller_nick
        FROM xqc_ods.alert_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
            AND toYYYYMMDD(toDate('{{ day.end=today }}'))
        AND shop_id GLOBAL IN (
            -- 已订阅店铺
            SELECT tenant_id AS shop_id
            FROM xqc_dim.company_tenant
            WHERE company_id = '{{ company_id=5f73e9c1684bf70001413636 }}'
                AND platform = '{{ platform=tb }}'
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
        AND platform = '{{ platform=tb }}'
    ) AS platform_axis
) AS time_platform_axis
GLOBAL LEFT JOIN (
    SELECT
        day,
        seller_nick,
        count(DISTINCT id) as cnt
    FROM xqc_ods.alert_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
        AND toYYYYMMDD(toDate('{{ day.end=today }}'))
    AND shop_id GLOBAL IN (
        -- 已订阅店铺
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=5f73e9c1684bf70001413636 }}'
            AND platform = '{{ platform=tb }}'
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
    AND platform = '{{ platform=tb }}'
    GROUP BY day, seller_nick
)
USING day, seller_nick
ORDER BY day ASC