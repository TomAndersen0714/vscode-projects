-- Impala
-- min(date)=2020-12-31
-- DROP TABLE dipper_dwd.ask_order_conversion_stat_day
CREATE TABLE dipper_dwd.ask_order_conversion_stat_day (
    shop_id STRING,
    ao_category STRING, -- "消息回复分类"
    ao_total_consult_order_cuv BIGINT, -- "询单人数"
    ao_ordered_cuv BIGINT, -- "下单人数"
    ao_paid_cuv BIGINT, -- "成单人数"
    ao_ordered_volume BIGINT, -- "订单数"
    ao_sold_money_volume FLOAT, -- "销售金额"
    ao_avg_transaction_value FLOAT -- "客单价"
) PARTITIONED BY (day INT) 
STORED AS PARQUET 
LOCATION 'hdfs://nameservice1/user/hive/warehouse/dipper_dwd.db/ask_order_conversion_stat_day'

-- Insert locally
INSERT OVERWRITE TABLE dipper_dwd.ask_order_conversion_stat_day
PARTITION(`day`={{ ds_nodash }})
SELECT
    shop_id,
    category as ao_category,  -- "消息回复占比"
    total_consult_order_cuv as ao_total_consult_order_cuv,  -- "询单人数"
    ordered_cuv as ao_ordered_cuv, -- "下单人数"
    paid_cuv as ao_paid_cuv, -- "成单人数"
    ordered_volume as ao_ordered_volume, -- "订单数"
    CAST(round(sold_money_volume / 100, 2) AS FLOAT) as ao_sold_money_volume,  -- "销售金额"
    CAST(round(average_transaction_value / 100, 2) AS FLOAT) as ao_avg_transaction_value -- "客单价"
FROM 
    app_mp.ask_order_conversion_nick
WHERE 
    `date` = '{{ ds }}'
    and 
    category in ('cate_all','cate_robot_ordered')
-- Example
INSERT OVERWRITE TABLE dipper_dwd.ask_order_conversion_stat_day
PARTITION(`day`=20201231)
SELECT
    shop_id,
    category as ao_category,  -- "消息回复占比"
    total_consult_order_cuv as ao_total_consult_order_cuv,  -- "询单人数"
    ordered_cuv as ao_ordered_cuv, -- "下单人数"
    paid_cuv as ao_paid_cuv, -- "成单人数"
    ordered_volume as ao_ordered_volume, -- "订单数"
    CAST(round(sold_money_volume / 100, 2) AS FLOAT) as ao_sold_money_volume,  -- "销售金额"
    CAST(round(average_transaction_value / 100, 2) AS FLOAT) as ao_avg_transaction_value -- "客单价"
FROM 
    app_mp.ask_order_conversion_nick
WHERE 
    `date` = '2020-12-31'
    and 
    category in ('cate_all','cate_robot_ordered')

-- Send
SELECT *,'tb' as platform
FROM dipper_dwd.ask_order_conversion_stat_day
WHERE day =  {ds_nodash}
