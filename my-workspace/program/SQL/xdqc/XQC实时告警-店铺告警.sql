-- 店铺告警统计-告警会话数
select 
    count(distinct dialog_id) as `告警会话数`, 
    count() as `告警会话次数`  
from xqc_ods.event_alert_all
where platform='{{ platform=tb }}' and
day = toYYYYMMDD(addDays(now(), {{ alert_date=0 }})) 
AND ((length('{{ seller_nick }}')=0
        AND shop_id GLOBAL IN
          (SELECT tenant_id AS shop_id
           FROM xqc_dim.company_tenant
           WHERE company_id = '{{ company_id=60dd5e791597f82cd050da9f }}'
             AND platform='{{ platform=tb }}'
              order by shop_id desc
           LIMIT 1))
       OR (length('{{ seller_nick }}') >0
           AND seller_nick='{{ seller_nick }}'))

-- 店铺告警统计-告警次数
select 
    count(distinct dialog_id) as `告警会话数`, 
    count() as `告警会话次数`  
from xqc_ods.event_alert_all
where platform='{{ platform=tb }}' and
day = toYYYYMMDD(addDays(now(), {{ alert_date=0 }})) 
AND ((length('{{ seller_nick }}')=0
        AND shop_id GLOBAL IN
          (SELECT tenant_id AS shop_id
           FROM xqc_dim.company_tenant
           WHERE company_id = '{{ company_id=60dd5e791597f82cd050da9f }}'
             AND platform='{{ platform=tb }}'
              order by shop_id desc
           LIMIT 1))
       OR (length('{{ seller_nick }}') >0
           AND seller_nick='{{ seller_nick }}'))

-- 店铺告警统计-告警详情
SELECT substring(dialog_time, 1, 19) AS `会话时间`,
       cnick AS `顾客`,
       snick AS `客服`,
       reason AS `告警类型`,
       substring(create_time, 1, 19) AS `告警时间`,
       dialog_id,
       platform
FROM xqc_ods.event_alert_all
WHERE day = toYYYYMMDD(addDays(now(), {{ alert_date=0 }}))
  AND type {{ alert_type=>0 }}
  AND platform='{{ platform=tb }}'
  AND ((length('{{ seller_nick }}')=0
        AND shop_id GLOBAL IN
          (SELECT tenant_id AS shop_id
           FROM xqc_dim.company_tenant
           WHERE company_id = '{{ company_id=60dd5e791597f82cd050da9f }}'
             AND platform='{{ platform=tb }}'
             order by shop_id desc
           LIMIT 1))
       OR (length('{{ seller_nick }}') >0
           AND seller_nick='{{ seller_nick }}'))
ORDER BY create_time DESC