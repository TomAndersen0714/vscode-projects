-- 淘宝-融合版/老淘宝(impala)
-- 全部: category=cate_all
-- 机器人回复后下单: category=cate_robot_ordered
SELECT shop_id,
    `date` AS "日期",
    category,
    total_consult_order_cuv as "询单人数",
    ordered_cuv as "下单人数",
    paid_cuv as "成单人数",
    round(paid_rate * 100, 2) as "成交占比",
    ordered_volume as "订单数",
    round(sold_money_volume / 100, 2) as "销售金额",
    round(average_transaction_value / 100, 2) as "客单价",
    round(conversion_rate * 100, 2) as "询单转化率",
    `date` AS create_time
FROM app_mp.ask_order_conversion_nick
WHERE `date` between '{{ day.start=week_ago }}' and '{{ day.end=yesterday }}'
    and shop_id = '{{ shop_id=5cac112e98ef4100118a9c9f }}'
    and category = '{{ category=cate_all }}'
order by `date` desc

-- 老淘宝/融合版源表:impala(kudu)
SELECT
    shop_id,
    category,
    total_consult_order_cuv, -- "询单人数"
    ordered_cuv, -- "下单人数"
    paid_cuv, -- "成单人数"
    round(paid_rate * 100, 2), -- "成交占比"
    ordered_volume, -- "订单数"
    round(sold_money_volume / 100, 2) , -- "销售金额"
    round(average_transaction_value / 100, 2) , -- "客单价"
    round(conversion_rate * 100, 2) , -- "询单转化率"
FROM app_mp.ask_order_conversion_nick
WHERE `date` = '{{ ds_dash }}' 
    and category in ('cate_all','cate_robot_ordered')

-- 老淘宝/融合版目标表:impala(parquet)
DROP TABLE dipper_dwd.ask_order_conversion_stat_day
CREATE TABLE dipper_dwd.ask_order_conversion_stat_day(
    shop_id STRING,
    ao_category STRING, -- "消息回复占比"
    ao_total_consult_order_cuv BIGINT, -- "询单人数"
    ao_ordered_cuv BIGINT, -- "下单人数"
    ao_paid_cuv BIGINT, -- "成单人数"
    ao_ordered_volume BIGINT, -- "订单数"
    ao_sold_money_volume DOUBLE, -- "销售金额"
    ao_avg_transaction_value DOUBLE -- "客单价"
)
PARTITIONED BY (day INT)
STORED AS PARQUET 
LOCATION 'hdfs://zjk-bigdata002:8020/user/hive/warehouse/dipper_dwd.db/ask_order_conversion_stat_day'
-- 老淘宝/融合版Insert SQL
INSERT OVERWRITE TABLE dipper_dwd.ask_order_conversion_stat_day
PARTITION(`day`={{ ds_nodash }})
SELECT
    shop_id,
    category as ao_category,  -- "消息回复占比"
    total_consult_order_cuv as ao_total_consult_order_cuv,  -- "询单人数"
    ordered_cuv as ao_ordered_cuv, -- "下单人数"
    paid_cuv as ao_paid_cuv, -- "成单人数"
    ordered_volume as ao_ordered_volume, -- "订单数"
    round(sold_money_volume / 100, 2) as ao_sold_money_volume,  -- "销售金额"
    round(average_transaction_value / 100, 2) as ao_avg_transaction_value -- "客单价"
FROM 
    app_mp.ask_order_conversion_nick
WHERE 
    `date` = '{{ ds }}'
    and 
    category in ('cate_all','cate_robot_ordered')

-- 测试
INSERT OVERWRITE TABLE dipper_dwd.ask_order_conversion_stat_day
PARTITION(`day`= 20210809)
SELECT
    shop_id,
    category as ao_category,  -- "消息回复占比"
    total_consult_order_cuv as ao_total_consult_order_cuv,  -- "询单人数"
    ordered_cuv as ao_ordered_cuv, -- "下单人数"
    paid_cuv as ao_paid_cuv, -- "成单人数"
    ordered_volume as ao_ordered_volume, -- "订单数"
    round(sold_money_volume / 100, 2) as ao_sold_money_volume,  -- "销售金额"
    round(average_transaction_value / 100, 2) as ao_avg_transaction_value -- "客单价"
FROM 
    app_mp.ask_order_conversion_nick
WHERE 
    `date` = '2021-08-09' 
    and 
    category in ('cate_all','cate_robot_ordered')


-- 京东(clickhouse)
-- 全部: ask_type=ALL
-- 机器人回复后下单: ask_type=robot_last
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
        parseDateTimeBestEffort('{{ day.start=week_ago }}')
    ) and toYYYYMMDD(
        parseDateTimeBestEffort('{{ day.end=yesterday }}')
    )
    and shop_id = '{{ shop_id=5de650c946e7c3001814990f }}'
    and ask_type = '{{ ask_type=ALL }}'
order by "日期" desc

-- 京东数据源:clickhouse
select
    shop_id,
    day,
    ask_type as ao_category, -- "消息回复分类"
    `ask_uv` as ao_total_consult_order_cuv, -- "询单人数"
    `create_uv` as ao_ordered_cuv, -- "下单人数"
    `paid_uv` as ao_paid_cuv, -- "成交人数"
    `order_cnt` as ao_ordered_volume, -- "订单数"
    round(`payment_value` / 100, 2) as ao_sold_money_volume, -- "销售额"
    if(paid_uv = 0, 0, round(`payment_value` / 100 / paid_uv, 2)) as ao_avg_transaction_value -- "客单价"
from pub_app_mp.shop_ask_order_cov_all
where 
    day ={{ ds_nodash }}
    and ask_type in ('ALL','robot_last')

-- 京东目标表:impala(parquet)
DROP TABLE dipper_dwd.ask_order_conversion_stat_day
CREATE TABLE dipper_dwd.ask_order_conversion_stat_day(
    shop_id STRING,
    ao_category STRING, -- "消息回复分类"
    ao_total_consult_order_cuv BIGINT, -- "询单人数"
    ao_ordered_cuv BIGINT, -- "下单人数"
    ao_paid_cuv BIGINT, -- "成单人数"
    ao_ordered_volume BIGINT, -- "订单数"
    ao_sold_money_volume DOUBLE, -- "销售金额"
    ao_avg_transaction_value DOUBLE -- "客单价"
)
PARTITIONED BY (day INT)
STORED AS PARQUET 
LOCATION 'hdfs://yd-bigdata-01:8020/user/hive/warehouse/dipper_dwd.db/ask_order_conversion_stat_day'

-- 京东临时表:impala(parquet)
DROP TABLE tmp.ask_order_conversion_stat_day
CREATE TABLE tmp.ask_order_conversion_stat_day(
    shop_id STRING,
    ao_category STRING, -- "消息回复占比"
    ao_total_consult_order_cuv BIGINT, -- "询单人数"
    ao_ordered_cuv BIGINT, -- "下单人数"
    ao_paid_cuv BIGINT, -- "成单人数"
    ao_ordered_volume BIGINT, -- "订单数"
    ao_sold_money_volume DOUBLE, -- "销售金额"
    ao_avg_transaction_value DOUBLE, -- "客单价"
    day INT
)
STORED AS PARQUET 
LOCATION 'hdfs://yd-bigdata-01:8020/user/hive/warehouse/tmp.db/ask_order_conversion_stat_day'

-- SELECT SQL
INSERT OVERWRITE TABLE dipper_dwd.ask_order_conversion_stat_day
PARTITION(`day`= {{ ds_nodash }}})
SELECT 
    shop_id,
    ao_category,
    ao_total_consult_order_cuv,
    ao_ordered_cuv,
    ao_paid_cuv,
    ao_ordered_volume,
    ao_sold_money_volume,
    ao_avg_transaction_value
FROM tmp.ask_order_conversion_stat_day