-- 全局告警统计
-- 告警趋势图(全局告警统计)
SELECT day as `日期`,
    `淘宝`,
    `京东`
FROM (
        SELECT DISTINCT day
        FROM xqc_ods.event_alert_all
        WHERE day > toYYYYMMDD(addDays(now(), -7))
    )
    LEFT JOIN (
        SELECT day,
            sum(platform = 'tb') as `淘宝`,
            sum(platform = 'jd') as `京东`
        FROM xqc_ods.event_alert_all
        WHERE day > toYYYYMMDD(addDays(now(), -8))
            AND shop_id GLOBAL IN (
                SELECT tenant_id AS shop_id
                FROM xqc_dim.company_tenant
                WHERE company_id = '{{company_id}}'
            )
        GROUP BY day
    ) USING day
ORDER BY day

-- 告警统计图(全局告警统计)
SELECT platform AS `平台`,
       count(1) AS `告警次数`,
       sum(type=1) AS `违禁词`,
       sum(type=2) AS `单句响应慢`,
       sum(type=3) AS `回复严重超时`,
       sum(type=4) AS `漏跟进`,
       sum(type=5) AS `违反广告法`,
       sum(type=6) AS `反问/质疑顾客`,
       sum(type=7) AS `对客服态度不满`,
       sum(type=8) AS `买家辱骂`,
       sum(type=9) AS `差评或要挟差评`,
       sum(type=10) AS `投诉或第三方曝光`
FROM xqc_ods.event_alert_all
WHERE day = toYYYYMMDD(addDays(now(), {{ alert_date=0 }}))
  AND shop_id GLOBAL IN
    (SELECT tenant_id AS shop_id
     FROM xqc_dim.company_tenant
     WHERE company_id = '{{ company_id=60dd5e791597f82cd050da9f }}')
GROUP BY platform

-- 平台告警统计
-- 告警趋势(平台告警统计)
select day AS `日期`,
    seller_nick,
    count
from (
        select *
        from (
                SELECT distinct day
                FROM xqc_ods.event_alert_all
                WHERE day > toYYYYMMDD(addDays(now(), -7))
            )
            cross join (
                SELECT distinct seller_nick
                FROM xqc_ods.event_alert_all
                WHERE shop_id GLOBAL IN (
                        SELECT tenant_id AS shop_id
                        FROM xqc_dim.company_tenant
                        WHERE company_id = '{{ company_id=60dd5e791597f82cd050da9f }}'
                            and platform = '{{ platform=tb }}'
                    )
            )
    )
    left join (
        SELECT day,
            seller_nick,
            count() as count
        FROM xqc_ods.event_alert_all
        WHERE day > toYYYYMMDD(addDays(now(), -8))
            AND shop_id GLOBAL IN (
                SELECT tenant_id AS shop_id
                FROM xqc_dim.company_tenant
                WHERE company_id = '{{ company_id=60dd5e791597f82cd050da9f }}'
                    and platform = '{{ platform=tb }}'
            )
        GROUP BY day,
            seller_nick
    ) using(day, seller_nick)
order by day

-- 告警统计(平台告警统计)
SELECT platform AS `平台`,
       seller_nick AS `店铺`,
       count(1) AS `告警次数`,
       sum(type=1) AS `违禁词`,
       sum(type=2) AS `单句响应慢`,
       sum(type=3) AS `回复严重超时`,
       sum(type=4) AS `漏跟进`,
       sum(type=5) AS `违反广告法`,
       sum(type=6) AS `反问/质疑顾客`,
       sum(type=7) AS `对客服态度不满`,
       sum(type=8) AS `买家辱骂`,
       sum(type=9) AS `差评或要挟差评`,
       sum(type=10) AS `投诉或第三方曝光`
FROM xqc_ods.event_alert_all
WHERE day = toYYYYMMDD(addDays(now(), {{ alert_date=0 }}))
  AND shop_id GLOBAL IN
    (SELECT tenant_id AS shop_id
     FROM xqc_dim.company_tenant
     WHERE company_id = '{{ company_id=60dd5e791597f82cd050da9f }}' AND platform = '{{ platform=tb }}')
GROUP BY platform,
         seller_nick