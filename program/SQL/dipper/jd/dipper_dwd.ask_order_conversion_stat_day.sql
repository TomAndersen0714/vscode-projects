-- Impala
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
LOCATION 'hdfs://yd-bigdata-01:8020/user/hive/warehouse/dipper_dwd.db/ask_order_conversion_stat_day'