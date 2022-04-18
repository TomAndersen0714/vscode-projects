-- 新实时告警-店铺告警-告警详情列表
SELECT
    seller_nick AS `店铺`, -- 店铺
    superior_name AS `客服负责人`, -- 负责人
    employee_name AS `客服`, -- 客服
    snick AS `子账号`, -- 子账号
    cnick AS `顾客`, -- 顾客
    CASE
        WHEN level=1 THEN '初级告警'
        WHEN level=2 THEN '中级告警'
        WHEN level=3 THEN '高级告警'
        ELSE '其他'
    END AS `告警等级`, -- 告警等级
    warning_type AS `告警内容`, -- 告警内容
    time AS `告警时间`, -- 告警时间
    toInt64(if(
        is_finished='True',
        round((parseDateTimeBestEffort(if(finish_time!='',finish_time,toString(now()))) - parseDateTimeBestEffort(time))/60),
        round((now() - parseDateTimeBestEffort(time))/60)
    )) AS `告警时长(min)`, -- 告警时长(min)
    finish_time AS `处理完成时间`, -- 处理完成时间
    if(is_finished='True','已处理','未处理') AS `处理状态`, -- 处理状态
    dialog_id, -- 会话ID(反查)
    message_id, -- 消息ID(反查)
    id AS alert_id, -- 告警ID(反查)
    platform,day,shop_id
FROM xqc_ods.alert_all FINAL
    -- 已订阅店铺
WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=today }}')) 
    AND toYYYYMMDD(toDate('{{ day.end=today }}'))
-- 过滤旧版标准
AND level IN [1,2,3]
AND shop_id GLOBAL IN (
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=5f73e9c1684bf70001413636 }}'
    AND platform = '{{ platform=tb }}'
)
    -- 权限隔离
AND (
        shop_id IN splitByChar(',','{{ shop_id_list=5f73e9c1684bf70001413636 }}')
        OR
        snick IN splitByChar(',','{{ snick_list=null }}')
    )
    -- 下拉框筛选
AND platform = '{{ platform=tb }}'
AND seller_nick='{{ shop_name=杜可风按 }}'
AND if({{ level=-1 }}!=-1,level = {{ level=-1 }}, level >=1)
AND if('{{ warning_type=全部 }}'!='全部',warning_type = '{{ warning_type=全部 }}', warning_type !='')
AND if('{{ is_finished=全部 }}'!='全部',is_finished = '{{ is_finished=全部 }}', is_finished!='')
    -- 文本框内容筛选
AND (
    superior_name LIKE '%{{ search_string }}%'
    OR
    snick LIKE '%{{ search_string }}%'
    OR
    cnick LIKE '%{{ search_string }}%'
)
order by time desc