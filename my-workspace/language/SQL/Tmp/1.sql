CREATE TABLE `mammut_user.ods_web_logs_di`(
    `id` string COMMENT '日志id',
    `content` string COMMENT '日志内容'
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION 'hdfs://easyops-cluster/user/mammut_user/hive_db/mammut_user.db/ods_web_logs_di' TBLPROPERTIES (
    'COLUMN_STATS_ACCURATE' = '{\"BASIC_STATS\":\"true\"}',
    'mammut.table.owner' = 'bdms_358966102',
    'numFiles' = '0',
    'numRows' = '0',
    'rawDataSize' = '0',
    'totalSize' = '0',
    'transient_lastDdlTime' = '1723344933'
)