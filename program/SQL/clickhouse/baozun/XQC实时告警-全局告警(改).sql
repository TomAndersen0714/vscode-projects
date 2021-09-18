-- 全局告警统计

-- 告警等级下拉框(全局告警统计)
等级固定为初级,中级,高级, 默认为全部

-- 告警项下拉框(全局告警统计)
大数据端只能查询已经发生的告警项, 不能查询实时设置的, 查询实时的得通过后端接口
修改: 前端表示表示无法查询后端接口, 因此需要直接查询大数据端
SELECT DISTINCT warning_type
FROM xqc_ods.event_alert_all
WHERE day BETWEEN toYYYYMMDD(toDate({{day.start=week_ago}}))
    AND toYYYYMMDD(toDate({{day.end=today}}))
AND shop_id GLOBAL IN (
    -- 已订阅店铺
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=60dd5e791597f82cd050da9f }}'
)
-- 权限隔离
AND (
        shop_id IN splitByChar(',','{{shop_id_list= }}') 
        OR
        snick IN splitByChar(',','{{snick_list= }}')
    )
-- 下拉框筛选
AND if('{{level=全部}}'!='全部',level={{level}},level >= 0) -- 告警等级


-- 告警趋势图(全局告警统计)
SELECT 
    day AS `日期`,
    CASE
        WHEN platform='jd' THEN '京东'
        WHEN platform='tb' THEN '淘宝'
        WHEN platform='ks' THEN '快手'
        WHEN platform='dy' THEN '抖音'
        ELSE '其他'
    END AS `平台`,
    platform_alert_daily_count -- 平台告警统计
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
        platform,
        count(1) AS platform_alert_daily_count
    FROM xqc_ods.event_alert_all FINAL
    WHERE day BETWEEN toYYYYMMDD(toDate({{day.start=week_ago}}))
        AND toYYYYMMDD(toDate({{day.end=today}}))
        -- 已订阅店铺
        AND shop_id GLOBAL IN (
            SELECT tenant_id AS shop_id
            FROM xqc_dim.company_tenant
            WHERE company_id = '{{company_id=60dd5e791597f82cd050da9f}}'
        )
        -- 权限隔离
        AND (
                shop_id IN splitByChar(',','{{shop_id_list= }}') 
                OR
                snick IN splitByChar(',','{{snick_list= }}')
            )
        -- 下拉框筛选
        AND if('{{level=全部}}'!='全部',level={{level}},level >= 0) -- 告警等级
        AND if('{{warning_type}}'!='全部',warning_type={{warning_type}},warning_type!='') -- 告警内容
    GROUP BY day, platform
)
USING day
ORDER BY day ASC, platform ASC


-- 告警统计列表-告警总量(全局告警统计)
SELECT 
    count(1) AS alert_daily_count
FROM xqc_ods.event_alert_all FINAL
WHERE day BETWEEN toYYYYMMDD(toDate({{day.start=week_ago}})) 
    AND toYYYYMMDD(toDate({{day.end=today}}))
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{company_id=60dd5e791597f82cd050da9f}}'
    )
    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{shop_id_list= }}') 
            OR
            snick IN splitByChar(',','{{snick_list= }}')
        )
    -- 下拉框筛选
    AND if('{{level=全部}}'!='全部',level={{level}},level!=0) -- 告警等级
    AND if('{{warning_type}}'!='全部',warning_type={{warning_type}},warning_type!='') -- 告警内容


-- 告警统计列表(全局告警统计)

-- 平台时间段内告警总量, 日均告警量
SELECT
    platform,
    count(1) AS platform_warning_cnt, -- 平台对应时间段内的告警总量
    dateDiff('day',toDate('{{day.start=week_ago}}'),toDate('{{day.end=today}}')) AS interval,
    -- 平台对应时间段内的日均告警量
    if(interval>0, round(platform_warning_cnt/interval,2), 0.00) AS platform_warning_cnt_avg
FROM xqc_ods.event_alert_all
WHERE day BETWEEN toYYYYMMDD(toDate({{day.start=week_ago}})) 
    AND toYYYYMMDD(toDate({{day.end=today}}))
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
          SELECT tenant_id AS shop_id
          FROM xqc_dim.company_tenant
          WHERE company_id = '{{ company_id=60dd5e791597f82cd050da9f }}'
       )
    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{shop_id_list= }}') 
            OR
            snick IN splitByChar(',','{{snick_list= }}')
        )
    -- 下拉框筛选
    AND if('{{level=全部}}'!='全部',level={{level}},level!=0) -- 告警等级
    AND if('{{warning_type}}'!='全部',warning_type={{warning_type}},warning_type!='') -- 告警内容
GROUP BY platform

-- 平台每日告警项统计
SELECT
    platform AS `平台`,
    day AS `日期`,
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
FROM xqc_ods.event_alert_all
WHERE day BETWEEN toYYYYMMDD(toDate({{day.start=week_ago}})) 
    AND toYYYYMMDD(toDate({{day.end=today}}))
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
          SELECT tenant_id AS shop_id
          FROM xqc_dim.company_tenant
          WHERE company_id = '{{ company_id=60dd5e791597f82cd050da9f }}'
       )
    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{shop_id_list= }}') 
            OR
            snick IN splitByChar(',','{{snick_list= }}')
        )
GROUP BY platform, day
ORDER BY platform ASC, day DESC

-- 合并
SELECT
    platform AS `平台`,
    day AS `日期`,
    platform_warning_cnt AS `告警总量`, -- 平台时间段内的告警总量
    platform_warning_cnt_avg AS `日均告警量`, -- 平台时间段内的日均告警量
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
        platform,
        count(1) AS platform_warning_cnt, -- 平台对应时间段内的告警总量
        dateDiff('day',toDate('{{day.start=week_ago}}'),toDate('{{day.end=today}}')) AS interval,
        -- 平台对应时间段内的日均告警量
        if(interval>0, round(platform_warning_cnt/interval,2), 0.00) AS platform_warning_cnt_avg
    FROM xqc_ods.event_alert_all
    WHERE day BETWEEN toYYYYMMDD(toDate({{day.start=week_ago}})) 
        AND toYYYYMMDD(toDate({{day.end=today}}))
        -- 已订阅店铺
        AND shop_id GLOBAL IN (
            SELECT tenant_id AS shop_id
            FROM xqc_dim.company_tenant
            WHERE company_id = '{{ company_id=60dd5e791597f82cd050da9f }}'
        )
        -- 权限隔离
        AND (
                shop_id IN splitByChar(',','{{shop_id_list= }}') 
                OR
                snick IN splitByChar(',','{{snick_list= }}')
            )
        -- 下拉框筛选
        AND if('{{level=全部}}'!='全部',level={{level}},level!=0) -- 告警等级
        AND if('{{warning_type}}'!='全部',warning_type={{warning_type}},warning_type!='') -- 告警内容
    GROUP BY platform
)
GLOBAL JOIN (
    SELECT
        platform AS `平台`,
        day AS `日期`,
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
    FROM xqc_ods.event_alert_all
    WHERE day BETWEEN toYYYYMMDD(toDate({{day.start=week_ago}})) 
        AND toYYYYMMDD(toDate({{day.end=today}}))
        -- 已订阅店铺
        AND shop_id GLOBAL IN (
            SELECT tenant_id AS shop_id
            FROM xqc_dim.company_tenant
            WHERE company_id = '{{ company_id=60dd5e791597f82cd050da9f }}'
        )
        -- 权限隔离
        AND (
                shop_id IN splitByChar(',','{{shop_id_list= }}') 
                OR
                snick IN splitByChar(',','{{snick_list= }}')
            )
    GROUP BY platform, day
)
USING platfrom
ORDER BY platform ASC, day DESC