-- 平台告警统计

-- 平台告警统计-下拉框-告警等级
等级固定为初级,中级,高级, 默认为全部

-- 平台告警统计-下拉框-告警项
大数据端只能查询已经发生的告警项, 不能查询实时设置的, 查询实时的得通过后端接口
修改: 前端表示表示无法查询后端接口, 因此需要直接查询大数据端
SELECT DISTINCT 
    warning_type
FROM xqc_ods.alert_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=week_ago}}'))
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
AND if('{{level=全部}}'!='全部',level={{level}},level >= 0) -- 告警等级

-- 平台告警统计-下拉框-平台
SELECT
    CASE
        WHEN platform='jd' THEN '京东'
        WHEN platform='tb' THEN '淘宝'
        WHEN platform='ks' THEN '快手'
        WHEN platform='dy' THEN '抖音'
    END AS `平台`
FROM xqc_dim.company_tenant
WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'

-- 平台告警统计-告警趋势
-- PS: 指定时间段, 指定平台
SELECT
    day,
    seller_nick,
    cnt
FROM (
    SELECT arrayJoin(
        arrayMap(
            x->toYYYYMMDD(toDate(x)),
            range(toUInt32(toDate('{{day.start=week_ago}}')), toUInt32(toDate('{{day.end=today}}') + 1), 1)
        )
    ) AS day
) AS time_axis
GLOBAL LEFT JOIN (
    SELECT
        day,
        seller_nick,
        count() as cnt
    FROM xqc_ods.alert_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=week_ago}}'))
        AND toYYYYMMDD(toDate('{{day.end=today}}'))
    AND shop_id GLOBAL IN (
        -- 已订阅店铺
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
    AND if('{{level=全部}}'!='全部',level={{level}},level >= 0) -- 告警等级
    AND if('{{warning_type}}'!='全部',warning_type='{{warning_type}}',warning_type!='') -- 告警内容
    AND platform = '{{ platform=tb }}'
    GROUP BY day, seller_nick
)
USING day
ORDER BY day ASC

-- 平台告警统计-告警趋势(改)
-- PS: 指定时间段, 指定平台
SELECT
    day,
    seller_nick,
    cnt
FROM (
    SELECT * FROM (
        SELECT arrayJoin(
            arrayMap(
                x->toYYYYMMDD(toDate(x)),
                range(toUInt32(toDate('{{day.start=week_ago}}')), toUInt32(toDate('{{day.end=today}}') + 1), 1)
            )
        ) AS day
    ) AS time_axis
    GLOBAL CROSS JOIN (
        SELECT DISTINCT
            seller_nick
        FROM xqc_ods.alert_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=week_ago}}'))
            AND toYYYYMMDD(toDate('{{day.end=today}}'))
        AND shop_id GLOBAL IN (
            -- 已订阅店铺
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
        AND if('{{level=全部}}'!='全部',level={{level}},level >= 0) -- 告警等级
        AND if('{{warning_type}}'!='全部',warning_type='{{warning_type}}',warning_type!='') -- 告警内容
        AND platform = '{{ platform=tb }}'
    ) AS platform_axis
) AS time_platform_axis
GLOBAL LEFT JOIN (
    SELECT
        day,
        seller_nick,
        count() as cnt
    FROM xqc_ods.alert_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=week_ago}}'))
        AND toYYYYMMDD(toDate('{{day.end=today}}'))
    AND shop_id GLOBAL IN (
        -- 已订阅店铺
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
    AND if('{{level=全部}}'!='全部',level={{level}},level >= 0) -- 告警等级
    AND if('{{warning_type}}'!='全部',warning_type='{{warning_type}}',warning_type!='') -- 告警内容
    AND platform = '{{ platform=tb }}'
    GROUP BY day, seller_nick
)
USING day
ORDER BY day ASC

-- 平台告警统计-告警统计列表-告警总量
-- PS: 指定平台, 指定时间段
SELECT
    count(1) AS alert_platform_period_count
FROM xqc_ods.alert_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=week_ago}}')) 
    AND toYYYYMMDD(toDate('{{day.end=today}}'))
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{company_id=5f747ba42c90fd0001254404}}'
            AND platform = '{{ platform=tb }}'
    )
    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{ shop_id_list=5cac112e98ef4100118a9c9f }}') 
            OR
            snick IN splitByChar(',','{{ snick_list=方太官方旗舰店:柚子 }}')
        )
    -- 下拉框筛选
    AND if('{{level=全部}}'!='全部',level={{level}},level!=0) -- 告警等级
    AND if('{{warning_type}}'!='全部',warning_type='{{warning_type}}',warning_type!='') -- 告警内容
    AND platform = '{{platform= }}'


-- 平台告警统计-告警统计列表-告警总量,日均告警量
-- PS: 指定平台,指定告警等级,指定告警内容,指定时间段,每天,各个店铺
WITH (
    SELECT dateDiff('day',toDate('{{day.start=week_ago}}'),toDate('{{day.end=today}}'))
) AS interval
SELECT
    platform,
    seller_nick,
    count(1) AS platform_shop_warning_cnt, -- 指定平台,指定时间段,各个店铺的告警总量
    if(interval>0, round(platform_shop_warning_cnt/interval,2), 0.00) AS platform_shop_warning_cnt_avg
    -- 指定平台,指定时间段,各个店铺的日均告警总量
FROM xqc_ods.alert_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=week_ago}}')) 
    AND toYYYYMMDD(toDate('{{day.end=today}}'))
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
          SELECT tenant_id AS shop_id
          FROM xqc_dim.company_tenant
          WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
       )
    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{ shop_id_list=5cac112e98ef4100118a9c9f }}') 
            OR
            snick IN splitByChar(',','{{ snick_list=方太官方旗舰店:柚子 }}')
        )
    -- 下拉框筛选
    AND if('{{level=全部}}'!='全部',level={{level}},level!=0) -- 告警等级
    AND if('{{warning_type}}'!='全部',warning_type='{{warning_type}}',warning_type!='') -- 告警内容
    AND platform = '{{platform= }}'
GROUP BY day,seller_nick

-- 平台告警统计-告警统计列表-各告警项统计
-- PS: 指定平台, 指定时间段, 每天, 每个店铺
-- PS: 后续需要支持34个告警项
SELECT
    platform AS `平台`,
    day AS `日期`,
    seller_nick AS `店铺`,
    sum(warning_type='违禁词') AS `违禁词`,
    sum(warning_type='单句响应慢') AS `单句响应慢`,
    sum(warning_type='回复严重超时') AS `回复严重超时`,
    sum(warning_type='漏跟进') AS `漏跟进`,
    sum(warning_type='违反广告法') AS `违反广告法`,
    sum(warning_type='反问/质疑顾客') AS `反问/质疑顾客`,
    sum(warning_type='对客服态度不满') AS `对客服态度不满`,
    sum(warning_type='买家辱骂') AS `买家辱骂`,
    sum(warning_type='差评或要挟差评') AS `差评或要挟差评`,
    sum(warning_type='投诉或第三方曝光') AS `投诉或第三方曝光`
FROM xqc_ods.alert_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=week_ago}}')) 
    AND toYYYYMMDD(toDate('{{day.end=today}}'))
-- 已订阅店铺
AND shop_id GLOBAL IN (
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{company_id=5f747ba42c90fd0001254404}}'
        AND platform = '{{ platform=tb }}'
)
-- 权限隔离
AND (
        shop_id IN splitByChar(',','{{ shop_id_list=5cac112e98ef4100118a9c9f }}') 
        OR
        snick IN splitByChar(',','{{ snick_list=方太官方旗舰店:柚子 }}')
    )
-- 下拉框筛选
AND if('{{level=全部}}'!='全部',level={{level}},level!=0) -- 告警等级
AND if('{{warning_type}}'!='全部',warning_type='{{warning_type}}',warning_type!='') -- 告警内容
AND platform = '{{platform= }}'
GROUP BY platform, day, seller_nick

-- 合并
-- PS: 告警总量落到店铺维度, 告警统计落到店铺-天维度, 过滤条件都是平台,时间,告警等级,告警项
WITH (
    SELECT dateDiff('day',toDate('{{day.start=week_ago}}'),toDate('{{day.end=today}}'))
) AS interval
SELECT
    platform AS `平台`,
    day AS `日期`,
    seller_nick AS `店铺`,
    platform_shop_warning_cnt AS `告警总量`, -- 指定平台指定时间段各个店铺内的告警总量
    platform_shop_warning_cnt_avg AS `日均告警量`, -- 指定平台指定时间段内各个店铺的日均告警量
    `违禁词`,
    `单句响应慢`,
    `回复严重超时`,
    `漏跟进`,
    `违反广告法`,
    `反问/质疑顾客`,
    `对客服态度不满`,
    `买家辱骂`,
    `差评或要挟差评`,
    `投诉或第三方曝光`
FROM (
    SELECT
        seller_nick,
        count(1) AS platform_shop_warning_cnt, -- 指定平台指定时间段各个店铺内的告警总量
        if(interval>0, round(platform_shop_warning_cnt/interval,2), 0.00) AS platform_shop_warning_cnt_avg
        -- 指定平台指定时间段内各个店铺的日均告警量
    FROM xqc_ods.alert_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=week_ago}}')) 
        AND toYYYYMMDD(toDate('{{day.end=today}}'))
        -- 已订阅店铺
        AND shop_id GLOBAL IN (
            SELECT tenant_id AS shop_id
            FROM xqc_dim.company_tenant
            WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        )
        -- 权限隔离
        AND (
                shop_id IN splitByChar(',','{{ shop_id_list=5cac112e98ef4100118a9c9f }}') 
                OR
                snick IN splitByChar(',','{{ snick_list=方太官方旗舰店:柚子 }}')
            )
        -- 下拉框筛选
        AND if('{{level=全部}}'!='全部',level={{level}},level!=0) -- 告警等级
        AND if('{{warning_type}}'!='全部',warning_type='{{warning_type}}',warning_type!='') -- 告警内容
        AND platform = '{{platform= }}'
    GROUP BY seller_nick
)
GLOBAL JOIN(
    SELECT
        platform,
        day,
        seller_nick,
        sum(warning_type='违禁词') AS `违禁词`,
        sum(warning_type='单句响应慢') AS `单句响应慢`,
        sum(warning_type='回复严重超时') AS `回复严重超时`,
        sum(warning_type='漏跟进') AS `漏跟进`,
        sum(warning_type='违反广告法') AS `违反广告法`,
        sum(warning_type='反问/质疑顾客') AS `反问/质疑顾客`,
        sum(warning_type='对客服态度不满') AS `对客服态度不满`,
        sum(warning_type='买家辱骂') AS `买家辱骂`,
        sum(warning_type='差评或要挟差评') AS `差评或要挟差评`,
        sum(warning_type='投诉或第三方曝光') AS `投诉或第三方曝光`
    FROM xqc_ods.alert_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=week_ago}}')) 
        AND toYYYYMMDD(toDate('{{day.end=today}}'))
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{company_id=5f747ba42c90fd0001254404}}'
            AND platform = '{{ platform=tb }}'
    )
    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{ shop_id_list=5cac112e98ef4100118a9c9f }}') 
            OR
            snick IN splitByChar(',','{{ snick_list=方太官方旗舰店:柚子 }}')
        )
    -- 下拉框筛选
    AND if('{{level=全部}}'!='全部',level={{level}},level!=0) -- 告警等级
    AND if('{{warning_type}}'!='全部',warning_type='{{warning_type}}',warning_type!='') -- 告警内容
    AND platform = '{{platform= }}'
    GROUP BY day, seller_nick
)
USING seller_nick
ORDER BY day DESC, seller_nick ASC