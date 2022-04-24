-- 新实时告警-店铺告警-实时告警项
SELECT
    `level`, -- 告警等级
    warning_type as `实时告警项`, -- 告警项描述
    sum(1) AS `告警总量`, -- 各告警项总量
    sum(is_finished = 'False') AS `未处理量`, -- 各告警项未处理总量
    if(`告警总量`!=0, round((`告警总量`-`未处理量`)/`告警总量`*100,2), 0.00) AS `完结率`
    -- 各告警项完结率
FROM xqc_ods.alert_all FINAL
WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=today }}')) 
    AND toYYYYMMDD(toDate('{{ day.end=today }}'))
    -- 已订阅店铺
AND shop_id GLOBAL IN (
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=5f73e9c1684bf70001413636 }}'
    AND platform = '{{ platform=tb }}'
)
    -- 权限隔离
AND (
        shop_id IN splitByChar(',','{{ shop_id_list=5bfe7a6a89bc4612f16586a5,5e7dbfa6e4f3320016e9b7d1 }}')
        OR
        snick IN splitByChar(',','{{ snick_list=null }}')
    )
    -- 下拉框筛选
AND platform = '{{ platform=tb }}'
AND seller_nick='{{ shop_name=杜可风按 }}'
AND if({{ level=-1 }}!=-1,level={{ level=-1 }},level IN [1,2,3]) -- 告警等级
AND if('{{ warning_type }}'!='全部',warning_type = '{{ warning_type }}', warning_type !='') -- 告警内容
GROUP BY `level`, warning_type
ORDER BY `level` DESC, warning_type ASC