-- 全局告警-统计列表
SELECT
    platform AS `平台`,
    day AS `日期`,
    platform_warning_daily_cnt AS `告警总量`, -- 平台时间段内的告警总量
    platform_warning_cnt_daily_avg AS `日均告警量`, -- 平台时间段内的日均告警量
    `非客服结束会话`, `漏跟进`, `快捷短语重复`, `生硬拒绝`, `欠缺安抚`, `答非所问`, `单字回复`,
    `单句响应慢`, `产品不熟悉`, `活动不熟悉`, `回复严重超时`, `撤回人工消息`, `单表情回复`,
    `异常撤回`, `转接前未有效回复`, `超时未回复`, `顾客撤回`, `前后回复矛盾`, `撤回机器人消息`, 
    `第三方投诉或曝光`, `顾客提及投诉或举报`, `差评或要挟差评`, `反问/质疑顾客`, `违禁词`, `客服冷漠讥讽`,
    `顾客怀疑假货`, `客服态度消极敷衍`, `售后不满意`, `对客服态度不满`, `对发货物流不满`, `对产品不满`, 
    `其他不满意`, `顾客骂人`,`客服骂人` ,`对收货少件不满`
FROM (
    -- "平台"维度聚合
    SELECT
        platform,
        count(DISTINCT day) AS interval, -- 告警天数
        if(interval>0, round(count(1)/interval,2), 0.00) AS platform_warning_cnt_daily_avg
        -- 各个平台对应时间段内的日均告警量
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
    GROUP BY platform
)
GLOBAL JOIN (
    -- "平台--天"维度聚合
    SELECT
        platform,
        day,
        sum(warning_type!='') AS platform_warning_daily_cnt,
        sum(warning_type='非客服结束会话') AS `非客服结束会话`,
        sum(warning_type='漏跟进') AS `漏跟进`,
        sum(warning_type='快捷短语重复') AS `快捷短语重复`,
        sum(warning_type='生硬拒绝') AS `生硬拒绝`,
        sum(warning_type='欠缺安抚') AS `欠缺安抚`,
        sum(warning_type='答非所问') AS `答非所问`,
        sum(warning_type='单字回复') AS `单字回复`,
        sum(warning_type='单句响应慢') AS `单句响应慢`,
        sum(warning_type='产品不熟悉') AS `产品不熟悉`,
        sum(warning_type='活动不熟悉') AS `活动不熟悉`,
        sum(warning_type='回复严重超时') AS `回复严重超时`,
        sum(warning_type='撤回人工消息') AS `撤回人工消息`,
        sum(warning_type='单表情回复') AS `单表情回复`,
        sum(warning_type='异常撤回') AS `异常撤回`,
        sum(warning_type='转接前未有效回复') AS `转接前未有效回复`,
        sum(warning_type='超时未回复') AS `超时未回复`,
        sum(warning_type='顾客撤回') AS `顾客撤回`,
        sum(warning_type='前后回复矛盾') AS `前后回复矛盾`,
        sum(warning_type='撤回机器人消息') AS `撤回机器人消息`,
        sum(warning_type='第三方投诉或曝光') AS `第三方投诉或曝光`,
        sum(warning_type='顾客提及投诉或举报') AS `顾客提及投诉或举报`,
        sum(warning_type='差评或要挟差评') AS `差评或要挟差评`,
        sum(warning_type='反问/质疑顾客') AS `反问/质疑顾客`,
        sum(warning_type='违禁词') AS `违禁词`,
        sum(warning_type='客服冷漠讥讽') AS `客服冷漠讥讽`,
        sum(warning_type='顾客怀疑假货') AS `顾客怀疑假货`,
        sum(warning_type='客服态度消极敷衍') AS `客服态度消极敷衍`,
        sum(warning_type='售后不满意') AS `售后不满意`,
        sum(warning_type='对客服态度不满') AS `对客服态度不满`,
        sum(warning_type='对发货物流不满') AS `对发货物流不满`,
        sum(warning_type='对产品不满') AS `对产品不满`,
        sum(warning_type='其他不满意') AS `其他不满意`,
        sum(warning_type='顾客骂人') AS `顾客骂人`,
        sum(warning_type='客服骂人') AS `客服骂人`,
        sum(warning_type='对收货少件不满') AS `对收货少件不满`
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
    GROUP BY platform, day
)
USING platform
ORDER BY platform ASC, day DESC