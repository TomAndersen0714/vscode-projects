-- 新实时告警-平台告警-告警总量
SELECT
    count(DISTINCT id) AS `告警总量`
FROM xqc_ods.alert_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) 
    AND toYYYYMMDD(toDate('{{ day.end=today }}'))
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            AND platform = '{{ platform=tb }}'
    )
    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{ shop_id_list=5cac112e98ef4100118a9c9f }}') 
            OR
            snick IN splitByChar(',','{{ snick_list=方太官方旗舰店:柚子 }}')
        )
    -- 下拉框筛选
    AND if({{ level=-1 }}!=-1,level={{ level=-1 }},level!=0) -- 告警等级
    AND if('{{ warning_type=全部 }}'!='全部',warning_type='{{ warning_type=全部 }}',warning_type!='') -- 告警内容
    AND platform = '{{ platform=tb }}'