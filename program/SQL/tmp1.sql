-- DROP TABLE IF EXISTS tmp.kudu_test
CREATE TABLE IF NOT EXISTS tmp.kudu_test (
  uuid STRING NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
  info STRING NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
  PRIMARY KEY (uuid)
) STORED AS KUDU TBLPROPERTIES ('kudu.master_addresses' = 'cdh2')


DROP TABLE IF EXISTS tmp.parquet_test
CREATE TABLE IF NOT EXISTS tmp.parquet_test
LIKE tmp.chat_log_v1
stored as parquet

insert overwrite table tmp.parquet_test partition(year,month,day) select * from tmp.chat_log_v1 limit 1000000;
