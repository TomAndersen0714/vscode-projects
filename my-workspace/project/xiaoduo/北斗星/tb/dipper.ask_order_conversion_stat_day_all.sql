CREATE TABLE dipper.ask_order_conversion_stat_day_local on cluster cluster_3s_2r( 
`day` Int32,
`shop_id` String,  
`platform` String, 
`ao_category` String,  
`ao_total_consult_order_cuv` Int64, 
`ao_ordered_cuv` Int64, 
`ao_paid_cuv` Int64, 
`ao_ordered_volume` Int64, 
`ao_sold_money_volume` Float32, 
`ao_avg_transaction_value` Float32
)
ENGINE = ReplicatedMergeTree('/clickhouse/dipper/tables/{layer}_{shard}/ask_order_conversion_stat_day_local', '{replica}')  
PARTITION BY day 
ORDER BY (shop_id,platform) SETTINGS index_granularity = 8192

CREATE TABLE dipper.ask_order_conversion_stat_day_all on cluster cluster_3s_2r
AS dipper.ask_order_conversion_stat_day_local
ENGINE = Distributed('cluster_3s_2r', 'dipper', 'ask_order_conversion_stat_day_local', rand())