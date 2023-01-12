-- ClickHouse
select 
    toString(toDate(parseDateTimeBestEffort(toString(day)))) as "日期",
    ask_type,
    `ask_uv` as "询单人数",
    `create_uv` as "下单人数",
    `paid_uv` as "成交人数",
    round(paid_uv / ask_uv * 100, 2) as "成交占比",
    `order_cnt` as "订单数",
    round(`payment_value` / 100, 2) as "销售额",
    `paid_payment` as "销售量",
    if(paid_uv = 0, 0, round(`payment_value` / 100 / paid_uv, 2)) as "客单价",
    if(ask_uv = 0, 0, round(paid_uv / ask_uv * 100, 2)) as "转化率"
from pub_app_mp.shop_ask_order_cov_all
where day between toYYYYMMDD(
        parseDateTimeBestEffort('{{ start_day }}')
    ) and toYYYYMMDD(
        parseDateTimeBestEffort('{{ end_day }}')
    )
    and shop_id = '{{ shop_id }}'
    and ask_type = '{{ ask_type=ALL }}'
order by "日期" desc
-- 全部: ALL
-- 人工回复占比超过70%: robot_30
-- 机器人回复占比超过50%: robot_50
-- 机器人回复占比超过70%: robot_70
-- 纯机器人回复: robot_100
-- 机器人回复后下单: robot_last