-- 基于LTP进行分词/词性标注/词性过滤


-- Phase2
DROP TABLE IF EXISTS tmp.intent_question_keyword_tokenized_ltp;
CREATE TABLE IF NOT EXISTS tmp.intent_question_keyword_tokenized_ltp(
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
    msg_words STRING COMMENT 'msg分词和词性标注结果',
    PRIMARY KEY(`uuid`,`day`)
)
PARTITION BY Hash(`day`) PARTITIONS 16
STORED AS KUDU;

-- Phase3
DROP TABLE IF EXISTS tmp.intent_question_cutted_filtered_ltp;
CREATE TABLE IF NOT EXISTS tmp.intent_question_cutted_filtered_ltp(
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
    msg_words STRING COMMENT 'msg分词和词性标注结果',
    filtered_msg_words_topk STRING COMMENT 'keyword中top3词性过滤',
    filtered_msg_words_default STRING COMMENT '默认词性过滤',
    PRIMARY KEY(`uuid`,`day`)
)
PARTITION BY Hash(`day`) PARTITIONS 16
STORED AS KUDU;

