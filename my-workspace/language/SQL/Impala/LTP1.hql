-- 提取日志中成功匹配到行业场景(行业问题)的买家问题信息
SELECT
    platform AS '平台',
    shop_id AS '店铺ID',
    question_b_qid AS '行业场景ID',
    question_b_standard_q AS '行业场景描述',
    msg AS '买家问题',
    `day`
FROM tmp.chat_log_v1
WHERE
    question_b_qid <> ''
    AND
    CAST(question_b_qid AS INT) > 3
    AND
    act = 'recv_msg'
    AND
    `day` = 20200819
    AND
    msg <> ''
    AND
    question_b_standard_q <> ''
ORDER BY
    CAST(question_b_qid AS INT)
LIMIT 100

-- 样本数据表
-- DROP TABLE IF EXISTS tmp.chat_log_sample;
CREATE TABLE IF NOT EXISTS tmp.chat_log_sample(
    platform STRING COMMENT '平台',
    shop_id STRING COMMENT '店铺ID',
    question_b_qid STRING COMMENT '行业场景ID',
    question_b_standard_q STRING COMMENT '行业场景描述',
    msg STRING COMMENT '买家问题'
)
PARTITIONED BY (`day` INT)
STORED AS PARQUET;

-- 插入样本数据
INSERT OVERWRITE TABLE tmp.chat_log_sample
PARTITION(`day`)
SELECT
    platform,
    shop_id,
    msg,
    question_b_qid,
    question_b_standard_q,
    `day`
FROM tmp.chat_log_v1
WHERE
    question_b_qid = '7'
    AND
    act = 'recv_msg'
    AND
    `day` = 20200819
    AND
    msg <> ''
    AND
    question_b_standard_q <> ''
ORDER BY
    CAST(question_b_qid AS INT);


-- LTP词法分析和句法分析结果
-- DROP TABLE IF EXISTS tmp.chat_log_tokenized_ltp;
CREATE TABLE IF NOT EXISTS tmp.chat_log_tokenized_ltp(
    uuid STRING COMMENT 'Kudu Primary Key',
    `day` INT,
    platform STRING COMMENT '消息所属平台',
    shop_id STRING COMMENT '店铺ID',
    question_b_qid STRING COMMENT '行业场景qid',
    question_b_standard_q STRING COMMENT '精准意图所属行业场景',
    msg STRING COMMENT '买家问题',
    msg_tokenized_words STRING COMMENT 'msg分词和词性标注结果',
    msg_named_entity STRING COMMENT 'msg命名实体标注',
    msg_semantic_role STRING COMMENT 'msg语义角色标注',
    msg_dep_syntactic_relation STRING COMMENT 'msg依存句法关系',
    msg_semantic_dependency_relation STRING COMMENT 'msg语义依存关系',
    PRIMARY KEY(`uuid`,`day`)
)
PARTITION BY Hash(`day`) PARTITIONS 16
STORED AS KUDU;

-- 基于LTP词法分析(词性过滤)的关键词提取
-- DROP TABLE IF EXISTS tmp.chat_log_keyword_by_flag_ltp;
CREATE TABLE IF NOT EXISTS tmp.chat_log_keyword_by_flag_ltp(
    uuid STRING COMMENT 'Kudu Primary Key',
    `day` INT,
    platform STRING COMMENT '消息所属平台',
    shop_id STRING COMMENT '店铺ID',
    question_b_qid STRING COMMENT '行业场景qid',
    question_b_standard_q STRING COMMENT '精准意图所属行业场景',
    msg STRING COMMENT '买家问题',
    msg_tokenized_words STRING COMMENT 'msg分词和词性标注结果',
    msg_named_entity STRING COMMENT 'msg命名实体标注',
    msg_semantic_role STRING COMMENT 'msg语义角色标注',
    msg_dep_syntactic_relation STRING COMMENT 'msg依存句法关系',
    msg_semantic_dependency_relation STRING COMMENT 'msg语义依存关系',
    msg_keyword_by_flag_default STRING COMMENT '基于词法分析的关键词和词性提取(默认词性过滤)',
    PRIMARY KEY(`uuid`,`day`)
)
PARTITION BY Hash(`day`) PARTITIONS 16
STORED AS KUDU;
-- 基于LTP句法分析的关键词提取
-- DROP TABLE IF EXISTS tmp.chat_log_keyword_by_syntax_ltp;
CREATE TABLE IF NOT EXISTS tmp.chat_log_keyword_by_syntax_ltp(
    uuid STRING COMMENT 'Kudu Primary Key',
    `day` INT,
    platform STRING COMMENT '消息所属平台',
    shop_id STRING COMMENT '店铺ID',
    question_b_qid STRING COMMENT '行业场景qid',
    question_b_standard_q STRING COMMENT '精准意图所属行业场景',
    msg STRING COMMENT '买家问题',
    msg_tokenized_words STRING COMMENT 'msg分词和词性标注结果',
    msg_named_entity STRING COMMENT 'msg命名实体标注',
    msg_semantic_role STRING COMMENT 'msg语义角色标注',
    msg_dep_syntactic_relation STRING COMMENT 'msg依存句法关系',
    msg_semantic_dependency_relation STRING COMMENT 'msg语义依存关系',
    msg_keyword_by_syntax STRING COMMENT '基于句法分析的关键词和词性提取',
    PRIMARY KEY(`uuid`,`day`)
)
PARTITION BY Hash(`day`) PARTITIONS 16
STORED AS KUDU;