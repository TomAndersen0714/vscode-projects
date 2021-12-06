-- 提取日志中成功匹配到行业场景(行业问题)的买家问题信息
SELECT
    platform,
    shop_id,
    question_b_qid,
    question_b_standard_q,
    msg,
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
LIMIT 5000

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
    question_b_qid,
    question_b_standard_q,
    msg,
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
LIMIT 5000

-- Phase 1
