-- ClickHouse
select 
    `date` as "日期",
    sum(serve_cuv) as "服务人数",
    sum(received_cuv) as "接待人数",
    sum(received_pv) as "问题总数",
    sum(identified_pv) as "识别问题数",
    sum(auto_reply_pv) as "机器人回复数",
    sum(click_reply_pv) as "点击回复数",
    sum(auto_reply_pv+ click_reply_pv)  as "应答总数",
    if(sum(received_pv) <= 0,0,round(sum(identified_pv)/sum(received_pv)*100,2))   as "识别率",
    if(sum(received_pv) <= 0,0,round((sum(auto_reply_pv + click_reply_pv))/sum(received_pv)*100,2))  as "应答率"
from 
    app_mp.msg_day_platform_nick 
where  
    shop_oid ='{{ shop_id=5de650c946e7c3001814990f }}' 
    and `date` between '{{ day.start=week_ago }}' and '{{ day.end=yesterday }}'
group by `date`
order by `date` desc

-- Kudu(Source)
SELECT
    '{{ date }}' AS `date`,
    platform,
    shop_oid,
    xd_shop_nick,
    count(distinct cnick) as serve_cuv
FROM dwd.xdrs_logs
WHERE shop_oid = {{ shop_id }}
AND day = cast(replace('{{ date }}','-','') AS INT)
GROUP BY 1,2,3,4
