-- 创建 ods_event_log 处理临时函数 event_log_handler
drop temporary function if exists event_log_handler;
create temporary function event_log_handler as 'com.tomandersen.hive.udtfs.CustomUDTFEventLogHandler'
using jar '/opt/libs/hive-custom-component-1.0.jar';
-- using jar '/opt/libs/hive-custom-component-1.0-jar-with-dependencies.jar';

-- 查看函数 event_log_handler 详细使用说明
desc function extended event_log_handler;
-- 使用函数 event_log_handler 处理 ods_event_log
SELECT event_log_handler(line,'mid','uid','vc','vn','l','sr','os',
    'ar','md','ba','sv','g','hw','t','nw','ln','la') 
    AS 
    (mid_id,user_id,version_code,version_name,lang,source,os,
    area,model,brand,sdk_version,gmail,height_width,app_time,
    network,lng,lat,server_time,event_name,event_json)
FROM gmall.ods_event_log
WHERE dt='2020-06-12'
LIMIT 6;
-- 或
SET mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzoCodec;
EXPLAIN
SELECT event_log_handler(line,'mid','uid','vc','vn','l','sr','os',
    'ar','md','ba','sv','g','hw','t','nw','ln','la') 
    AS 
    (mid_id,user_id,version_code,version_name,lang,source,os,
    area,model,brand,sdk_version,gmail,height_width,app_time,
    network,lng,lat,server_time,event_name,event_json)
FROM (
    SELECT line
    FROM gmall.ods_event_log
    WHERE dt='2020-06-12'
    LIMIT 1
) AS tbl_1;

-- set hive.execution.engine=tez;
-- SELECT deptno, avg(sal) as avg_sal FROM emp group by deptno;


-- 创建 ods_event_log 预处理临时函数 event_log_base_handler
-- 可以获取事件日志中的任何Json Value,同时输出server_time
drop temporary function if exists event_log_base_handler;
create temporary function event_log_base_handler as 'com.tomandersen.hive.udtfs.CustomUDTFEventLogBaseHander'
using jar '/opt/libs/hive-custom-component-1.0.jar';
-- using jar '/opt/libs/hive-custom-component-1.0-jar-with-dependencies.jar';

-- 使用event_log_base_handler函数处理ods_event_log
SELECT event_log_base_handler(line,'cm','ap','et')
    AS (common_field,app,event_json,server_time)
FROM gmall.ods_event_log
WHERE dt='2020-06-12'
LIMIT 1;

-- 创建 ods_event_log 事件字段et处理临时函数 event_log_event_handler
-- 可以用于处理事件JSON数组,输出其中的所有事件
drop temporary function if exists event_log_event_handler;
create temporary function event_log_event_handler as 'com.tomandersen.hive.udtfs.CustomUDTFEventLogEventHandler'
using jar '/opt/libs/hive-custom-component-1.0.jar';
-- using jar '/opt/libs/hive-custom-component-1.0-jar-with-dependencies.jar';

-- 使用 event_log_event_handler 函数处理 ods_event_log 的事件字段
SELECT server_time,v_tbl_1.*
FROM (
    SELECT event_log_base_handler(line,'et') AS (events,server_time)
    FROM gmall.ods_event_log
    WHERE dt='2020-06-12'
    LIMIT 1
) AS tbl_1
LATERAL VIEW event_log_event_handler(events) v_tbl_1 as event_name,event_json;


-- 创建 ods_event_log 公共字段处理临时函数 event_log_common_handler
-- 可以用于处理事件日志的公共字段,输出其中的任意Json Value
drop temporary function if exists event_log_common_handler;
create temporary function event_log_common_handler as 'com.tomandersen.hive.udtfs.CustomUDTFEventLogCommonHandler'
using jar '/opt/libs/hive-custom-component-1.0.jar';
-- using jar '/opt/libs/hive-custom-component-1.0-jar-with-dependencies.jar';
-- 使用 event_log_common_handler 函数处理 ods_event_log 的公共字段
SELECT server_time,v_tbl_1.*
FROM (
    SELECT event_log_base_handler(line,'cm') AS (common_field,server_time)
    FROM gmall.ods_event_log
    WHERE dt='2020-06-12'
    LIMIT 1
) AS tbl_1
LATERAL VIEW event_log_common_handler(common_field,'mid','uid','vc','g') v_tbl_1 
AS mid_id,user_id,version_code,gmall;


-- 创建测试用表
DROP TABLE IF EXISTS tmp;
CREATE EXTERNAL TABLE tmp(line string)
STORED AS
-- 设置支持lzo索引的InputFormat,避免将lzo索引文件当成数据文件
-- 注意:Hive只支持使用旧版MapReduce API读取表数据
INPUTFORMAT "com.hadoop.mapred.DeprecatedLzoTextInputFormat"
OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";

-- 导入测试数据
load data local inpath '/tmp/hive/data/SteveJobs_speech.txt.lzo' overwrite into table tmp;
-- 为hdfs测试数据创建索引
hadoop jar \
/opt/module/hadoop-2.7.7/share/hadoop/common/hadoop-lzo-0.4.21-SNAPSHOT.jar \
com.hadoop.compression.lzo.DistributedLzoIndexer \
/tmp/data/input/SteveJobs_speech.txt.lzo
-- 查询表数据
SELECT * FROM tmp;
-- 结论:使用lzo压缩时,需要同时使用com.hadoop.mapred.DeprecatedLzoTextInputFormat作为表
-- 的InputFormat,避免将lzo索引文件也视为数据文件,同时支持lzo压缩文件分片


-- 设置MR输出文件压缩方式为lzo,后缀名为 .lzo_deflate
SET mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzoCodec;

-- 输出查询数据
INSERT OVERWRITE DIRECTORY '/tmp/data/output/events'
ROW FORMAT 
DELIMITED FIELDS TERMINATED BY '\t'
SELECT server_time,v_tbl_1.*
FROM (
    SELECT event_log_base_handler(line,'et') AS (events,server_time)
    FROM gmall.ods_event_log
    WHERE dt='2020-06-12'
    LIMIT 1
) AS tbl_1
LATERAL VIEW event_log_event_handler(events) v_tbl_1 as event_name,event_json;
-- 尝试给输出数据文件 .lzo_deflate 建立索引
hadoop jar \
/opt/module/hadoop-2.7.7/share/hadoop/common/hadoop-lzo-0.4.21-SNAPSHOT.jar \
com.hadoop.compression.lzo.DistributedLzoIndexer \
/tmp/data/output/events/000000_0.lzo_deflate
-- 输出:No input paths found - perhaps all .lzo files have already been indexed
-- 结论:只能对 .lzo 文件建立lzo索引,不能对 .lzo_deflate 文件建立索引


-- 重新建表,尝试查看 .deflate 文件
DROP TABLE IF EXISTS tmp;
CREATE TABLE tmp(line string);
-- 设置输出压缩方式为 org.apache.hadoop.io.compress.DeflateCodec
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.DeflateCodec;
-- 导出测试数据
INSERT OVERWRITE DIRECTORY '/tmp/data/output/emp'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT * FROM emp;
-- 将之前导出的查询结果的 .lzo_deflate 文件导入表中进行查询
LOAD DATA INPATH '/tmp/data/output/emp/000000_0.deflate' OVERWRITE INTO TABLE tmp;
-- 查询表数据
SELECT * FROM tmp;
-- 结论:可以使用 Deflate 压缩算法进行压缩和解压
-- PS:io.compression.codecs参数中需要设置对应编解码器


-- 重新建表,尝试查看 .lzo 文件
DROP TABLE IF EXISTS tmp;
CREATE TABLE tmp like emp;
-- 设置输出压缩方式为 org.apache.hadoop.io.compress.DeflateCodec
SET mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzopCodec;
-- 导出测试数据
INSERT OVERWRITE DIRECTORY '/tmp/data/output/emp'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT * FROM emp;
-- 将之前导出的查询结果的 .lzo_deflate 文件导入表中进行查询
LOAD DATA INPATH '/tmp/data/output/emp/000000_0.lzo' OVERWRITE INTO TABLE tmp;
-- 查询表数据
SELECT * FROM tmp;
-- 结论:可以使用 LzopCodec 编解码器进行压缩和解压
-- PS:io.compression.codecs参数中需要设置对应编解码器
-- 执行某些查询时却报错,如: 
SELECT deptno,avg(sal) AS avg_sal FROM tmp GROUP BY deptno;

-- 重新建表,尝试查看 .lzo_deflate 文件
DROP TABLE IF EXISTS tmp;
CREATE TABLE tmp like emp;
-- 设置MR Job的输出文件压缩方式为 .lzo_deflate
SET mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzoCodec;
-- 输出表数据
INSERT OVERWRITE DIRECTORY '/tmp/data/output/emp'
ROW FORMAT 
DELIMITED FIELDS TERMINATED BY '\t'
SELECT * FROM emp;
-- 导入数据
LOAD DATA INPATH '/tmp/data/output/emp/000000_0.lzo_deflate' OVERWRITE INTO TABLE tmp;
-- 查看表数据
SELECT * FROM tmp;
-- 结论:可以查看 .lzo_deflate 文件
-- PS:io.compression.codecs参数中需要设置对应编解码器



-- 可以通过以下命令,修改表数据的读取/写入/序列化和反序列化方式
ALTER TABLE tmp
SET FILEFORMAT
INPUTFORMAT "com.hadoop.mapred.DeprecatedLzoTextInputFormat"
OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';

-- 通过 gmall.dwd_event_log 表获取所有可能的事件(event_name)和事件抽样(event_json)
-- 按照 event_name 进行分组,选取组内首条数据
SELECT
    tbl_1.event_name,
    max(tbl_1.event_json)
FROM gmall.dwd_event_log AS tbl_1
GROUP BY tbl_1.event_name;
-- 或
SELECT
    tbl_1.event_name,
    tbl_1.event_json
FROM (
    SELECT
        *,
        row_number() OVER(PARTITION BY event_name) AS row_number
    FROM gmall.dwd_event_log
) AS tbl_1
WHERE tbl_1.row_number=1;



-- 测试
CREATE TABLE app_fishpond.customer_pool_stat_snapshot (
    shop_oid STRING,
    total_uv BIGINT,
    inflow_uv BIGINT,
    outflow_uv BIGINT,
    edge_uv BIGINT,
    active_total_uv BIGINT,
    active_inflow_uv BIGINT,
    active_outflow_uv BIGINT,
    active_edge_uv BIGINT
)
PARTITIONED BY (platform STRING)
STORED AS PARQUET LOCATION 'hdfs://zjk-bigdata002:8020/user/hive/warehouse/app_fishpond.db/customer_pool_stat_snapshot'

-- Kudu Test Table
DROP TABLE IF EXISTS tmp.kudu_test_1;
CREATE TABLE tmp.kudu_test_1 (
    id INT NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
    info STRING NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
    PRIMARY KEY (id)
) STORED AS KUDU TBLPROPERTIES (
    'kudu.master_addresses' = 'cdh2'
);