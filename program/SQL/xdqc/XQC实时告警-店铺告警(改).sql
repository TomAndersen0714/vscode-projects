-- 店铺告警统计

-- 店铺告警统计-下拉框-平台(单选,不支持全部)
SELECT
    CASE
        WHEN platform='jd' THEN '京东'
        WHEN platform='tb' THEN '淘宝'
        WHEN platform='ks' THEN '快手'
        ELSE '其他'
    END AS `平台`
FROM xqc_dim.company_tenant
WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
ORDER BY platform ASC

-- 店铺告警统计-下拉框-店铺(单选,不支持全部)
-- PS: 根据平台进行查询
SELECT DISTINCT 
    tenant_label AS shop_name
FROM xqc_dim.company_tenant
    -- 已订阅店铺
WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    -- 下拉框筛选
AND platform = '{{ platform=tb }}'
ORDER BY tenant_label ASC

-- 店铺告警统计-监控总量
-- PS: 指定平台,店铺
SELECT
    COUNT(1) AS dialog_cnt
FROM
    xqc_ods.dialog_all
WHERE shop_id GLOBAL IN (
    -- 已订阅店铺
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    AND platform = '{{ platform=tb }}'
)
AND (
    -- 权限隔离
        shop_id IN splitByChar(',','{{shop_id_list=5cac112e98ef4100118a9c9f }}')
        OR
        snick IN splitByChar(',','{{snick_list=方太官方旗舰店:柚子 }}')
    )
    -- 下拉框筛选
AND platform = '{{ platform=tb }}'
AND seller_nick='{{ shop_name=null }}'

-- 店铺告警统计-告警总量(此SQL和告警等级分布合并)
-- PS: 指定平台,店铺
SELECT
    COUNT(1) AS alert_cnt
FROM
    xqc_ods.alert_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=today}}')) 
    AND toYYYYMMDD(toDate('{{day.end=today}}'))
    -- 已订阅店铺
AND shop_id GLOBAL IN (
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    AND platform = '{{ platform=tb }}'
)
AND (
    -- 权限隔离
        shop_id IN splitByChar(',','{{shop_id_list=5cac112e98ef4100118a9c9f }}')
        OR
        snick IN splitByChar(',','{{snick_list=方太官方旗舰店:柚子 }}')
    )
    -- 下拉框筛选
AND platform = '{{ platform=tb }}'
AND seller_nick='{{ shop_name=null }}'

-- 店铺告警统计-告警会话量(此SQL和告警等级分布合并)
-- PS: 指定平台,店铺
SELECT 
    COUNT(DISTINCT dialog_id) AS alert_dialog_cnt
FROM
    xqc_ods.alert_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=today}}')) 
    AND toYYYYMMDD(toDate('{{day.end=today}}'))
    -- 已订阅店铺
AND shop_id GLOBAL IN (
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    AND platform = '{{ platform=tb }}'
)
AND (
    -- 权限隔离
        shop_id IN splitByChar(',','{{shop_id_list=5cac112e98ef4100118a9c9f }}')
        OR
        snick IN splitByChar(',','{{snick_list=方太官方旗舰店:柚子 }}')
    )
    -- 下拉框筛选
AND platform = '{{ platform=tb }}'
AND seller_nick='{{ shop_name=null }}'

-- 店铺告警统计-告警等级分布,告警总量,告警会话量
SELECT
    SUM(level=1) AS level_1_cnt, -- 初级告警总量
    SUM(level=2) AS level_2_cnt, -- 中级告警总量
    SUM(level=3) AS level_3_cnt, -- 高级告警总量
    level_1_cnt+level_2_cnt+level_3_cnt AS alert_cnt, -- 告警总量
    COUNT(DISTINCT dialog_id) AS alert_dialog_cnt -- 告警会话量
FROM
    xqc_ods.alert_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=today}}')) 
    AND toYYYYMMDD(toDate('{{day.end=today}}'))
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
AND platform = '{{ platform=tb }}'
AND seller_nick='{{ shop_name=null }}'

-- 店铺告警统计-告警等级分布(改)
SELECT
    CASE
        WHEN level=1 THEN '初级告警'
        WHEN level=2 THEN '中级告警'
        WHEN level=3 THEN '高级告警'
        ELSE '其他'
    END AS alert,
    count(1) AS alert_cnt
FROM
    xqc_ods.alert_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=today }}')) 
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
AND platform = '{{ platform=tb }}'
AND seller_nick='{{ shop_name=null }}'
group by level
order by level ASC, alert_cnt desc

-- 店铺告警统计-实时告警项
SELECT
    `level`, -- 告警等级
    warning_type, -- 告警项描述
    sum(is_finished = 'False') AS not_finished_cnt, -- 各告警项未处理总量
    sum(1) AS warning_cnt, -- 各告警项总量
    if(warning_cnt!=0, round((warning_cnt-not_finished_cnt)/warning_cnt*100,2), 0.00) AS warning_finished_ratio
    -- 各告警项完结率
FROM xqc_ods.alert_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=today}}')) 
    AND toYYYYMMDD(toDate('{{day.end=today}}'))
    -- 已订阅店铺
AND shop_id GLOBAL IN (
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    AND platform = '{{ platform=tb }}'
)
    -- 权限隔离
AND (
        shop_id IN splitByChar(',','{{shop_id_list=5cac112e98ef4100118a9c9f }}')
        OR
        snick IN splitByChar(',','{{snick_list=方太官方旗舰店:柚子 }}')
    )
    -- 下拉框筛选
AND platform = '{{ platform=tb }}'
AND seller_nick='{{ shop_name=null }}'
AND if({{ level=-1 }}!=-1,level={{ level=-1 }},level IN [1,2,3]) -- 告警等级
AND if('{{ warning_type=全部 }}'!='全部',warning_type = '{{warning_type}}', warning_type !='') -- 告警项目
GROUP BY `level`, warning_type
ORDER BY `level` DESC, warning_type ASC


-- 店铺告警统计-下拉框-告警等级(固定,默认为全部)
初级,中级,高级,全部(默认)

-- 店铺告警统计-下拉框-告警内容/告警项(默认全部)
-- PS: 需要根据'告警等级'进行指定, 由于是直接查询大数据端, 因此只能查询历史实际记录, 不能查实时配置
SELECT DISTINCT
    warning_type
FROM xqc_ods.alert_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=today}}')) 
    AND toYYYYMMDD(toDate('{{day.end=today}}'))
    -- 已订阅店铺
AND shop_id GLOBAL IN (
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    AND platform = '{{ platform=tb }}'
)
-- 权限隔离
AND (
        shop_id IN splitByChar(',','{{shop_id_list=5cac112e98ef4100118a9c9f }}')
        OR
        snick IN splitByChar(',','{{snick_list=方太官方旗舰店:柚子 }}')
    )
-- 下拉框筛选
AND platform = '{{ platform=tb }}'
AND seller_nick='{{ shop_name=null }}'
AND if('{{ level=全部 }}'!='全部',level = {{level}}, level >=1)



-- 店铺告警统计-下拉框-处理状态(固定,默认为全部)
已处理,未处理,全部(默认)

-- 店铺告警统计-文本框-搜索负责人/子账号/顾客昵称
用户填写,变量名为 search_string, 默认值为空

-- 店铺告警统计-告警详情列表
SELECT
    seller_nick AS shop_name, -- 店铺
    superior_name, -- 负责人
    employee_name, -- 客服
    snick, -- 子账号
    cnick, -- 顾客
    level, -- 告警等级
    warning_type, -- 告警内容
    time, -- 告警时间
    toInt64(if(
        is_finished='True',
        round((parseDateTimeBestEffort(if(finish_time!='',finish_time,toString(now()))) - parseDateTimeBestEffort(time))/60),
        round((now() - parseDateTimeBestEffort(time))/60)
    )) AS warning_duration, -- 告警时长(min)
    finish_time, -- 处理完成时间
    is_finished, -- 处理状态
    dialog_id, -- 会话ID(反查)
    message_id, -- 消息ID(反查)
    id AS alert_id -- 告警ID(反查)
FROM xqc_ods.alert_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=today }}')) 
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
        shop_id IN splitByChar(',','{{shop_id_list=5cac112e98ef4100118a9c9f }}')
        OR
        snick IN splitByChar(',','{{snick_list=方太官方旗舰店:柚子 }}')
    )
-- 下拉框筛选
AND platform = '{{ platform=tb }}'
AND seller_nick='{{ shop_name=null }}'
AND if('{{ level=全部 }}'!='全部',level = {{level}}, level >=1)
AND if('{{ warning_type=全部 }}'!='全部',warning_type = '{{warning_type}}', warning_type !='')
AND if('{{ is_finished=全部 }}'!='全部',is_finished = '{{ is_finished }}', is_finished!='')
    -- 文本框内容筛选
AND (
    superior_name LIKE '%{{search_string}}%'
    OR
    snick LIKE '%{{search_string}}%'
    OR
    cnick LIKE '%{{search_string}}%'
)



