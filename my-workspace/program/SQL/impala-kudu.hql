-- 用于练习impala-kudu操作

-- 建立Kudu到Impala表的映射,使得能够通过Impala直接查询Kudu中已存在表
-- 此处创建的是外部表,删除时不影响Kudu中原有的数据
DROP TABLE IF EXISTS `tmp.kudu_test_table`;
CREATE EXTERNAL TABLE `tmp.kudu_test_table`
STORED AS KUDU
TBLPROPERTIES(
    'kudu.table_name'='kudu_test_table',
    'kudu.master_addresses' = 'cdh0:7051,cdh1:7051,cdh2:7051'
);

SELECT * FROM `kudu_test_table`;

-- 或者也可以直接先在Impala中创建Kudu表,然后再向其中导入数据,如:
-- 此处创建的是内部表(管理表)
-- 注意: 在Impala中创建Kudu表时,必须指定PRIMARY KEY
DROP TABLE IF EXISTS `tmp.impala_kudu_test_table`
CREATE TABLE IF NOT EXISTS tmp.impala_kudu_test_table(
    `foo` BIGINT COMMENT 'key',
    `bar` TIMESTAMP COMMENT 'value',
    PRIMARY KEY(`foo`,`bar`)
)
STORED AS KUDU;
-- PS1:通过Impala创建Kudu表时,不需要关联Kudu中的表,即不设置kudu.table_name值
-- PS2:通过Impala创建Kudu表时,也不需要手动设置kudu.master_addresses属性,impala
-- 会默认配置.

INSERT OVERWRITE INTO tmp.impala_kudu_test_table
SELECT * FROM tmp.kudu_test_table;

-- 通过impala创建Kudu分区表
-- Hash分区表
-- PS: Kudu的Hash分区表不支持动态扩展,必须在声明表模式Schema的同时,规定分区字段和分区个数 \
-- 且Hash分区支持多个字段组合Hash,或者多个字段Hash组合
DROP TABLE IF EXISTS `impala_kudu_hash_partitioned_test_table`
CREATE TABLE IF NOT EXISTS `impala_kudu_hash_partitioned_test_table`(
    id INT,
    `day` INT,
    info STRING,
    PRIMARY KEY(`id`,`day`)
)
PARTITION BY HASH(id,`day`) PARTITIONS 16
STORED AS KUDU;
-- PS: Kudu表的Hash字段必须为Primary Key,Primary Key中的字段必须在表
-- 声明时的字段列表最前端,且顺序必须相同


-- 本地测试集群Phase-2测试用表
DROP TABLE IF EXISTS tmp.question_keyword_tokenized_test;
CREATE TABLE IF NOT EXISTS tmp.question_keyword_tokenized_test(
    uuid STRING,
    `day` INT,
    msg STRING,
    tokenized_keywords STRING,
    PRIMARY KEY(`uuid`,`day`)
)
STORED AS KUDU;

-- 本地测试集群Phase-2测试数据
-- PS: 将msg字段视为线上环境中的keyword字段,只为测试程序正确性
SELECT
    msg,`day`
FROM xd_data.chat_event
WHERE 
    `day`=20200720 
AND act='recv_msg'
AND precise_intent_id<> '' 
LIMIT 10

-- 本地测试集群Phase-3测试用表
DROP TABLE IF EXISTS tmp.question_msg_cutted_filtered_test;
CREATE TABLE IF NOT EXISTS tmp.question_msg_cutted_filtered_test(
    uuid STRING,
    `day` INT,
    msg STRING,
    tokenized_keywords STRING,
    filtered_msg_words_topk STRING,
    filtered_msg_words_default STRING,
    PRIMARY KEY(`uuid`,`day`)
)
PARTITION BY Hash(`day`) PARTITIONS 16
STORED AS KUDU;

