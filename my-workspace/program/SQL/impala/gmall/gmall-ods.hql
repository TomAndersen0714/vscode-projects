-- ODS层建表语句
-- 如果创建的表已经存在,则删除该表
DROP TABLE IF EXISTS gmall.ods_start_log;
-- 创建外部表,字段为单个string字段
CREATE EXTERNAL TABLE gmall.ods_start_log(line string) 
-- 按照日期进行分区
PARTITIONED BY (dt string)
STORED AS
-- 设置支持lzo索引的InputFormat,避免将lzo索引文件当成数据文件
-- 注意:Hive只支持使用旧版MapReduce API读取表数据
INPUTFORMAT "com.hadoop.mapred.DeprecatedLzoTextInputFormat" -- 表数据的读取方式
OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat" -- 表数据的输出方式
;
-- 加载hdfs中的数据到表所在路径,加载的是某指定日期的日志
LOAD DATA INPATH '/logs/app_start/2020-06-12' INTO TABLE ods_start_log PARTITION(dt='2020-06-12');


-- 删除已存在表
DROP TABLE IF EXISTS gmall.ods_event_log;
-- 创建外部表,字段为单个string字段
CREATE EXTERNAL TABLE gmall.ods_event_log(line string)
-- 按照日期进行分区
PARTITIONED BY (dt string)
STORED AS
-- 设置支持lzo索引的InputFormat,避免将lzo索引文件当成数据文件
-- 注意:Hive只支持使用旧版MapReduce API读取表数据
INPUTFORMAT "com.hadoop.mapred.DeprecatedLzoTextInputFormat" -- 表数据的读取方式
OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat" -- 表数据的输出方式
;
-- 加载hdfs中的数据到表所在路径,加载的是某指定日期的日志
LOAD DATA INPATH '/logs/app_event/2020-06-12' INTO TABLE ods_event_log PARTITION(dt='2020-06-12');
