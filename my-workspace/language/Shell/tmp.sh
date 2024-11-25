CREATE TABLE `mammut_user.test`
(
  `id` string
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://easyops-cluster/user/mammut_user/hive_db/mammut_user.db/test'
TBLPROPERTIES (
  'mammut.table.owner'='bdms_huangsibo', 
  'numFiles'='1', 
  'numRows'='0', 
  'rawDataSize'='0', 
  'spark.sql.partitionProvider'='catalog', 
  'totalSize'='50', 
  'transient_lastDdlTime'='1697788674')