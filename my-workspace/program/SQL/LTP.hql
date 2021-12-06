-- 基于LTP API进行词法分析(分词、词性标注),以及词性过滤


-- Phase1
-- DROP TABLE IF EXISTS tmp.msg_to_intent_question;
CREATE TABLE IF NOT EXISTS tmp.msg_to_intent_question(
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

-- Insert tmp.msg_to_intent_question
INSERT OVERWRITE tmp.msg_to_intent_question
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
        AND platform = 'tb'
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

-- 用于拉取数据到测试集群
-- DROP TABLE IF EXISTS tmp.msg_to_intent_question_tmp;
CREATE TABLE IF NOT EXISTS tmp.msg_to_intent_question_tmp(
    platform STRING COMMENT '消息所属平台',
    question_b_id STRING COMMENT '精准意图所属行业场景id',
    question_b_qid STRING COMMENT '精准意图所属行业场景qid',
    question_b_standard_q STRING COMMENT '精准意图所属行业场景',
    precise_intent_id STRING COMMENT '精准意图id',
    precise_intent_standard_q STRING COMMENT '精准意图描述',
    keyword STRING COMMENT '精准意图关键字',
    msg STRING COMMENT '与精准意图配对的买家问题',
    `day` INT
)
STORED AS PARQUET;
-- Insert tmp.msg_to_intent_question_tmp
INSERT OVERWRITE tmp.msg_to_intent_question_tmp
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


-- Phase2
-- DROP TABLE IF EXISTS tmp.msg_tokenized_ltp;
CREATE TABLE IF NOT EXISTS tmp.msg_tokenized_ltp(
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
    msg_tokenized_words STRING COMMENT 'msg分词和词性标注结果',
    msg_named_entity STRING COMMENT 'msg命名实体标注',
    msg_semantic_role STRING COMMENT 'msg语义角色标注',
    msg_dep_syntactic_relation STRING COMMENT 'msg依存句法关系',
    msg_semantic_dependency_relation STRING COMMENT 'msg语义依存关系',
    PRIMARY KEY(`uuid`,`day`)
)
PARTITION BY Hash(`day`) PARTITIONS 16
STORED AS KUDU;

-- Phase3
-- DROP TABLE IF EXISTS tmp.msg_keyword_by_syntax_ltp;
CREATE TABLE IF NOT EXISTS tmp.msg_keyword_by_syntax_ltp(
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
    msg_tokenized_words STRING COMMENT 'msg分词和词性标注结果',
    msg_keyword_by_syntax STRING COMMENT '基于句法分析的关键词和词性提取',
    PRIMARY KEY(`uuid`,`day`)
)
PARTITION BY Hash(`day`) PARTITIONS 16
STORED AS KUDU;

-- DROP TABLE IF EXISTS tmp.msg_keyword_by_flag_ltp;
CREATE TABLE IF NOT EXISTS tmp.msg_keyword_by_flag_ltp(
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
    msg_tokenized_words STRING COMMENT 'msg分词和词性标注结果',
    msg_keyword_by_flag_default STRING COMMENT '基于词法分析的关键词和词性提取(默认词性过滤)',
    msg_keyword_by_flag_topk STRING COMMENT '基于词法分析的关键词和词性提取(keyword中topk词性过滤)',
    PRIMARY KEY(`uuid`,`day`)
)
PARTITION BY Hash(`day`) PARTITIONS 16
STORED AS KUDU;


-- 对比关键词提取结果
SELECT
    t1.uuid,
    t1.platform AS '平台',
    t1.`day` AS '日期',
    t1.question_b_standard_q AS '行业场景',
    t1.precise_intent_standard_q AS '精准意图',
    t1.keyword AS '精准意图关键词',
    t1.tokenized_keywords AS '精准意图关键词词性标注',
    t1.msg AS '买家问题',
    t1.msg_tokenized_words AS '买家问题词性标注',
    t1.msg_keyword_by_flag_default AS '默认词性提取关键词',
    t1.msg_keyword_by_flag_topk AS 'keyword词性TopK提取关键词',
    t2.msg_keyword_by_syntax AS '句法分析提取关键词'
FROM
    tmp.msg_keyword_by_flag_ltp AS t1
LEFT JOIN
    tmp.msg_keyword_by_syntax_ltp AS t2
ON t1.uuid = t2.uuid;
