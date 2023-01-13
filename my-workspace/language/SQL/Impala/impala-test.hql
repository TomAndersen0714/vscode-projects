-- 用于存放各种测试SQL

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
-- PS: 将msg字段视为线上环境中的keyword字段,对其进行词性标注,只为测试程序正确性
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
    msg_words STRING,
    filtered_msg_words_topk STRING,
    filtered_msg_words_default STRING,
    PRIMARY KEY(`uuid`,`day`)
)
PARTITION BY Hash(`day`) PARTITIONS 16
STORED AS KUDU;
