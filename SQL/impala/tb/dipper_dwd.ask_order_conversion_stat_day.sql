-- min(date)=2019-10-01
-- DROP TABLE dipper_dwd.ask_order_conversion_stat_day
CREATE TABLE dipper_dwd.ask_order_conversion_stat_day(
    `day` INT,
    shop_id STRING,
    platform STRING,
    ao_category STRING, -- "消息回复分类"
    ao_total_consult_order_cuv BIGINT, -- "询单人数"
    ao_ordered_cuv BIGINT, -- "下单人数"
    ao_paid_cuv BIGINT, -- "成单人数"
    ao_ordered_volume BIGINT, -- "订单数"
    ao_sold_money_volume FLOAT, -- "销售金额"
    ao_avg_transaction_value FLOAT, -- "客单价"
    PRIMARY KEY(`day`, shop_id, platform, ao_category)
)
PARTITION BY HASH (shop_id) PARTITIONS 16
STORED AS KUDU TBLPROPERTIES (
    'kudu.master_addresses' = 'zjk-bigdata002'
)

-- Insert locally
UPSERT INTO dipper_dwd.ask_order_conversion_stat_day
SELECT
    {{ ds_nodash }} as day,
    shop_id,
    'tb' as platform,
    category as ao_category,  -- "消息回复占比"
    total_consult_order_cuv as ao_total_consult_order_cuv,  -- "询单人数"
    ordered_cuv as ao_ordered_cuv, -- "下单人数"
    paid_cuv as ao_paid_cuv, -- "成单人数"
    ordered_volume as ao_ordered_volume, -- "订单数"
    CAST(round(sold_money_volume / 100, 2) AS FLOAT) as ao_sold_money_volume,  -- "销售金额"
    CAST(round(average_transaction_value / 100, 2) AS FLOAT) as ao_avg_transaction_value -- "客单价"
FROM
    app_mp.ask_order_conversion_nick
WHERE `date` = '{{ ds }}'
    and category in ('cate_all','cate_robot_ordered')
-- Example
UPSERT INTO dipper_dwd.ask_order_conversion_stat_day
SELECT
    20191001 as day,
    shop_id,
    'tb' as platform,
    category as ao_category,  -- "消息回复占比"
    total_consult_order_cuv as ao_total_consult_order_cuv,  -- "询单人数"
    ordered_cuv as ao_ordered_cuv, -- "下单人数"
    paid_cuv as ao_paid_cuv, -- "成单人数"
    ordered_volume as ao_ordered_volume, -- "订单数"
    CAST(round(sold_money_volume / 100, 2) AS FLOAT) as ao_sold_money_volume,  -- "销售金额"
    CAST(round(average_transaction_value / 100, 2) AS FLOAT) as ao_avg_transaction_value -- "客单价"
FROM
    app_mp.ask_order_conversion_nick
WHERE `date` = '2019-10-01'
    and category in ('cate_all','cate_robot_ordered')


