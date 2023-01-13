-- -- 测试
-- WITH t1 AS (
--     SELECT msg,question_b_qid
--     FROM xd_data.chat_event
--     WHERE `day`=20200730
--     AND act='recv_msg'
--     AND precise_intent_id<> ''
--     LIMIT 10
-- )
-- SELECT
--     t3._id,
--     t1.question_b_qid,
--     t3.intent,
--     t3.keyword,
--     t1.msg
-- FROM
--     dim.question_b AS t2
-- INNER JOIN
--     t1
-- ON
--     CAST(t2.qid AS STRING) = t1.question_b_qid
-- INNER JOIN
--     xd_data.question_b_precise AS t3
-- ON
--     t3.question_b_id = t2._id
-- -- 测试

-- -- 实际运行(抽取20200730当天的待处理数据)
-- WITH t1 AS (
--     SELECT msg,question_b_qid
--     FROM xd_data.chat_event
--     WHERE `day`=20200730
--     AND act='recv_msg'
--     AND precise_intent_id<> ''
-- )
-- SELECT
--     t3._id,
--     t1.question_b_qid,
--     t3.intent,
--     t3.keyword,
--     t1.msg
-- FROM
--     dim.question_b AS t2
-- INNER JOIN
--     t1
-- ON
--     CAST(t2.qid AS STRING) = t1.question_b_qid
-- INNER JOIN
--     xd_data.question_b_precise AS t3
-- ON
--     t3.question_b_id = t2._id

-- -- V1
-- -- Phase-1
-- /* 创建Kudu表存放查询结果(弃用) */
-- DROP TABLE IF EXISTS tmp.question_keyword_origin;
-- CREATE TABLE IF NOT EXISTS tmp.question_keyword_origin(
--     _id STRING,
--     uuid STRING,
--     question_b_id STRING,
--     intent STRING,
--     keyword STRING,
--     msg STRING,
--     filtered_cutted_words STRING,
--     cutted_words STRING,
--     `day` INT,
--     PRIMARY KEY(`_id`,`uuid`)
-- )
-- STORED AS KUDU
-- TBLPROPERTIES(
--     'kudu.master_addresses' = 'cdh0:7051,cdh1:7051,cdh2:7051'
-- );

-- /*创建HDFS Parquet表存放查询结果(选用)*/
-- -- Design Table Schema
-- -- DROP TABLE IF EXISTS tmp.question_keyword_origin
-- CREATE TABLE IF NOT EXISTS tmp.question_keyword_origin(
--     _id STRING,
--     question_b_qid STRING,
--     intent STRING,
--     keyword STRING,
--     msg STRING
-- )
-- PARTITIONED BY (`day` STRING)
-- STORED AS PARQUET;

-- SHOW CREATE TABLE tmp.question_keyword_origin;

-- -- Insert Data(OK)
-- INSERT OVERWRITE tmp.question_keyword_origin
-- PARTITION(`day`)
-- WITH t1 AS (
--     SELECT msg,question_b_qid,`day`
--     FROM xd_data.chat_event
--     WHERE `day`=20200730
--     AND act='recv_msg'
--     AND precise_intent_id<> ''
-- )
-- SELECT
--     t2._id,
--     t1.question_b_qid,
--     t3.intent,
--     t3.keyword,
--     t1.msg,
--     t1.`day`
-- FROM
--     dim.question_b AS t2
-- INNER JOIN
--     t1
-- ON
--     CAST(t2.qid AS STRING) = t1.question_b_qid
-- INNER JOIN
--     xd_data.question_b_precise AS t3
-- ON
--     t3.question_b_id = t2._id;

-- -- Check Data
-- SELECT *
-- FROM tmp.question_keyword_origin
-- LIMIT 10;

-- SELECT * FROM tmp.question_keyword_origin
-- WHERE keyword='魅族,魅蓝'
-- LIMIT 10


-- -- Phase-2
-- /*HDFS Parquet表*/
-- -- 待定
-- -- DROP TABLE IF EXISTS tmp.question_keyword_tokenized
-- CREATE TABLE IF NOT EXISTS tmp.question_keyword_tokenized(
--     _id STRING,
--     question_b_qid STRING,
--     intent STRING,
--     keyword STRING,
--     msg STRING,
--     tokenized_keywords STRING
-- )
-- PARTITIONED BY (`day` INT)
-- STORED AS PARQUET;

-- /*impala-kudu 表*/
-- -- 暂定
-- DROP TABLE IF EXISTS tmp.question_keyword_tokenized;
-- CREATE TABLE IF NOT EXISTS tmp.question_keyword_tokenized(
--     uuid STRING,
--     `day` INT,
--     _id STRING,
--     question_b_qid STRING,
--     intent STRING,
--     keyword STRING,
--     msg STRING,
--     tokenized_keywords STRING,
--     PRIMARY KEY(`uuid`,`day`)
-- )
-- PARTITION BY Hash(`day`) PARTITIONS 16
-- STORED AS KUDU;

-- -- Phase-3
-- /*HDFS Parquet表*/
-- -- 待定
-- -- DROP TABLE IF EXISTS tmp.question_msg_cutted_filtered
-- CREATE TABLE IF NOT EXISTS tmp.question_msg_cutted_filtered(
--     _id STRING,
--     question_b_qid STRING,
--     intent STRING,
--     keyword STRING,
--     msg STRING,
--     tokenized_keywords STRING,
--     msg_words STRING,
--     filtered_msg_words_topk STRING,
--     filtered_msg_words_default STRING
-- )
-- PARTITIONED BY (`day` INT)
-- STORED AS PARQUET;
-- /*impala-kudu 表*/
-- -- 暂定
-- DROP TABLE IF EXISTS tmp.question_msg_cutted_filtered;
-- CREATE TABLE IF NOT EXISTS tmp.question_msg_cutted_filtered(
--     uuid STRING,
--     `day` INT,
--     _id STRING,
--     question_b_qid STRING,
--     intent STRING,
--     keyword STRING,
--     msg STRING,
--     tokenized_keywords STRING,
--     msg_words STRING,
--     filtered_msg_words_topk STRING,
--     filtered_msg_words_default STRING,
--     PRIMARY KEY(`uuid`,`day`)
-- )
-- PARTITION BY Hash(`day`) PARTITIONS 16
-- STORED AS KUDU;


-- V2

-- Phase1 V2:
DROP TABLE IF EXISTS tmp.intent_question_keyword_origin;
CREATE TABLE IF NOT EXISTS tmp.intent_question_keyword_origin(
    platform STRING COMMENT '消息所属平台',
    question_b_id STRING COMMENT '精准意图所属行业场景id',
    question_b_qid STRING COMMENT '精准意图所属行业场景qid',
    question_b_standard_q STRING COMMENT '精准意图所属行业场景',
    precise_intent_id STRING COMMENT '精准意图id',
    precise_intent_standard_q STRING COMMENT '精准意图描述',
    keyword STRING COMMENT '精准意图关键字',
    msg STRING COMMENT '与精准意图配对的买家问题'
)
PARTITIONED BY (`day` INT)
STORED AS PARQUET;

-- Query Test
WITH t1 AS (
    SELECT
        platform,
        question_b_id,
        question_b_qid,
        question_b_standard_q,
        precise_intent_id,
        precise_intent_standard_q,
        msg,
        `day`
    FROM
        xd_data.chat_event
    WHERE `day`=20200730
    AND act='recv_msg'
    AND precise_intent_id <> ''
    LIMIT 100
)
SELECT
    t1.platform,
    t1.question_b_id,
    t1.question_b_qid,
    t1.question_b_standard_q,
    t1.precise_intent_id,
    t1.precise_intent_standard_q,
    t2.keyword,
    t1.msg,
    t1.`day`
FROM
    t1
INNER JOIN
    xd_data.question_b_precise AS t2
ON
    t1.precise_intent_id = t2._id
LIMIT 100

-- Insert Data
INSERT OVERWRITE tmp.intent_question_keyword_origin
PARTITION(`day`)
WITH t1 AS (
    SELECT
        platform,
        question_b_id,
        question_b_qid,
        question_b_standard_q,
        precise_intent_id,
        precise_intent_standard_q,
        msg,
        `day`
    FROM
        xd_data.chat_event
    WHERE
        `day`=20200730
        AND act='recv_msg'
        AND precise_intent_id <> ''
)
SELECT
    t1.platform,
    t1.question_b_id,
    t1.question_b_qid,
    t1.question_b_standard_q,
    t1.precise_intent_id,
    t1.precise_intent_standard_q,
    t2.keyword,
    t1.msg,
    t1.`day`
FROM
    t1
INNER JOIN
    xd_data.question_b_precise AS t2
ON
    t1.precise_intent_id = t2._id;

-- Check Data
SELECT *
FROM tmp.intent_question_keyword_origin
LIMIT 100;

-- Phase2 V2:
DROP TABLE IF EXISTS tmp.intent_question_keyword_tokenized;
CREATE TABLE IF NOT EXISTS tmp.intent_question_keyword_tokenized(
    uuid STRING COMMENT 'Kudu Primary Key',
    `day` INT,
    platform STRING COMMENT '消息所属平台',
    question_b_id STRING COMMENT '精准意图所属行业场景id',
    question_b_qid STRING COMMENT '精准意图所属行业场景qid',
    question_b_standard_q STRING COMMENT '精准意图所属行业场景',
    precise_intent_id STRING COMMENT '精准意图id',
    precise_intent_standard_q STRING COMMENT '精准意图描述',
    keyword STRING COMMENT '精准意图关键字',
    tokenized_keywords STRING COMMENT 'keyword词性标注',
    msg STRING COMMENT '与精准意图配对的买家问题',
    msg_words STRING COMMENT 'msg分词结果',
    PRIMARY KEY(`uuid`,`day`)
)
PARTITION BY Hash(`day`) PARTITIONS 16
STORED AS KUDU;
-- Data Source
SELECT
    uuid,
    `day`,
    platform,
    question_b_id,
    question_b_qid,
    question_b_standard_q,
    precise_intent_id,
    precise_intent_standard_q,
    keyword,
    msg
FROM
    tmp.intent_question_keyword_origin
WHERE
    `day`=20200730
-- Query Test
SELECT
    *
FROM
    tmp.intent_question_keyword_tokenized
LIMIT 100

-- Phase3 V2
DROP TABLE IF EXISTS tmp.intent_question_cutted_filtered;
CREATE TABLE IF NOT EXISTS tmp.intent_question_cutted_filtered(
    uuid STRING COMMENT 'Kudu Primary Key',
    `day` INT,
    platform STRING COMMENT '消息所属平台',
    question_b_id STRING COMMENT '精准意图所属行业场景id',
    question_b_qid STRING COMMENT '精准意图所属行业场景qid',
    question_b_standard_q STRING COMMENT '精准意图所属行业场景',
    precise_intent_id STRING COMMENT '精准意图id',
    precise_intent_standard_q STRING COMMENT '精准意图描述',
    keyword STRING COMMENT '精准意图关键字',
    tokenized_keywords STRING COMMENT 'keyword词性标注',
    msg STRING COMMENT '与精准意图配对的买家问题',
    msg_words STRING COMMENT 'msg分词结果',
    filtered_msg_words_topk STRING COMMENT 'keyword中top3词性过滤',
    filtered_msg_words_default STRING COMMENT '默认词性过滤',
    PRIMARY KEY(`uuid`,`day`)
)
PARTITION BY Hash(`day`) PARTITIONS 16
STORED AS KUDU;

