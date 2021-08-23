DROP TABLE dipper_dwd.ask_order_conversion_stat_day
CREATE TABLE dipper_dwd.ask_order_conversion_stat_day (
  shop_id STRING,
  ao_category STRING,
  ao_total_consult_order_cuv BIGINT,
  ao_ordered_cuv BIGINT,
  ao_paid_cuv BIGINT,
  ao_ordered_volume BIGINT,
  ao_sold_money_volume FLOAT,
  ao_avg_transaction_value FLOAT
) PARTITIONED BY (day INT) STORED AS PARQUET 
LOCATION 'hdfs://zjk-bigdata002:8020/user/hive/warehouse/dipper_dwd.db/ask_order_conversion_stat_day'



CREATE TABLE tmp.hive_type_test_tbl (
  string_type STRING,
  int_type INT,
  bigint_type BIGINT,
  float_type FLOAT,
  double_type DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://cdh0:8020/user/hive/warehouse/tmp.db/hive_test_tbl'

INSERT OVERWRITE tmp.hive_type_test_tbl values('1',1,1,CAST(round(135/100,0) AS FLOAT),1.1)

INSERT INTO 