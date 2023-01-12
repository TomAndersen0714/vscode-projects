alter table dipper_ods.ask_order_conversion_stat_day set tblproperties('EXTERNAL'='true');
alter table dipper_ods.ask_order_conversion_stat_day set tblproperties('EXTERNAL'='false');
ALTER TABLE dipper_ods.ask_order_conversion_stat_day SET TBLPROPERTIES('kudu.table_name' = 'impala::dipper_ods.ask_order_conversion_stat_day')

kudu table rename_table zjk-bigdata002:7051 impala::dipper_dwd.ask_order_conversion_stat_day impala::dipper_ods.ask_order_conversion_stat_day


ALTER TABLE dipper_ods.ask_order_conversion_stat_day 
SET TBLPROPERTIES('EXTERNAL' = 'TRUE')

ALTER TABLE dipper_ods.ask_order_conversion_stat_day 
SET TBLPROPERTIES('EXTERNAL' = 'FALSE')


CREATE TABLE tmp.hdfs_type_test(
    str_type STRING,
    int_type INT
)
STORED AS PARQUET 
LOCATION 'hdfs://cdh0:8020/user/hive/warehouse/tmp.db/hdfs_type_test'


-- Kudu
CREATE TABLE dipper_ods.ask_order_conversion_stat_day(
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
    'kudu.master_addresses' = 'cdh2:7051,cdh1:7051,cdh0:7051'
)

-- HDFS Parquet
CREATE TABLE tmp.ask_order_conversion_stat_day(
    `day` INT,
    shop_id STRING,
    platform STRING,
    ao_category STRING, -- "消息回复分类"
    ao_total_consult_order_cuv BIGINT, -- "询单人数"
    ao_ordered_cuv BIGINT, -- "下单人数"
    ao_paid_cuv BIGINT, -- "成单人数"
    ao_ordered_volume BIGINT, -- "订单数"
    ao_sold_money_volume FLOAT, -- "销售金额"
    ao_avg_transaction_value FLOAT -- "客单价"
)
STORED AS PARQUET 
LOCATION 'hdfs://zjk-bigdata002:8020/user/hive/warehouse/tmp.db/ask_order_conversion_stat_day'
-- LOCATION 'hdfs://cdh0:8020/user/hive/warehouse/tmp.db/ask_order_conversion_stat_day'


INSERT OVERWRITE tmp.ask_order_conversion_stat_day
SELECT * FROM dipper_ods.ask_order_conversion_stat_day
WHERE day=20210829


