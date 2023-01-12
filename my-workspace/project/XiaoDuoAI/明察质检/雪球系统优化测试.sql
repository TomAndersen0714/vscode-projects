-- 1. 创建测试表
-- DROP TABLE tmp.test1 
CREATE TABLE tmp.test1 (
    snick STRING,
    cnick STRING,
    qid_stat STRING,
    big STRING,
    question STRING
) 
SORT BY (snick, cnick) 
STORED AS PARQUET 
LOCATION 'hdfs://nameservice1/user/hive/warehouse/tmp.db/test1' 

-- 2. 查看其表属性
DESCRIBE EXTENDED tmp.test1;
-- 小结: 发现 SORT BY 在表属性(TBLPROPERTIES)中的变量名为 sort.columns

-- 3. 尝试修改表属性
ALTER TABLE tmp.test1 SET TBLPROPERTIES('sort.columns' = 'snick')
-- 小结: 证明此变量是可修改的

-- 4. 重建测试表, 测试此变量是否支持手动添加
DROP TABLE tmp.test1 
CREATE TABLE tmp.test1 (
    snick STRING,
    cnick STRING,
    qid_stat STRING,
    big STRING,
    question STRING
) 
STORED AS PARQUET 
LOCATION 'hdfs://nameservice1/user/hive/warehouse/tmp.db/test1'
-- 手动修改变量
ALTER TABLE tmp.test1 SET TBLPROPERTIES('sort.columns' = 'snick')
-- 查看建表语句
SHOW CREATE TABLE tmp.test1
-- 小结: 说明 SORT BY 子句支持渐变之后创建和修改.
-- PS: 后续还需要使用实际聊天数据来测试性能.

-- 5. 索引性能测试
老淘宝中Impala的聊天日志数据落到OSS盘/opt/bigdata/,线下创建两个不同的表结构,即排序和不排序
排序表的创建要在原始表导入数据文件之后, 使用"CREATE TABLE AS"或者"INSERT INTO"

-- 创建聊天测试表1
CREATE TABLE test.mini_xdrs_log (
    snick STRING,
    shop_id STRING,
    cnick STRING,
    msg STRING,
    act STRING,
    mode STRING,
    msg_time BIGINT,
    category STRING,
    platform STRING,
    mp_version STRING,
    qa_id STRING,
    is_identified INT,
    plat_goods_id STRING,
    shop_question_type STRING,
    shop_question_id STRING,
    question_b_qid STRING,
    question_b_proba DOUBLE,
    question_b_standard_q STRING,
    intent STRING,
    current_sale_stage STRING,
    is_robot_answer INT,
    create_time STRING,
    answer_id STRING,
    answer_explain STRING,
    question_b_id STRING,
    remind_answer STRING,
    msg_id STRING,
    task_id STRING,
    answer_source STRING,
    send_msg_from STRING,
    nick STRING,
    checked_order_mark BOOLEAN,
    is_precise_intent BOOLEAN,
    precise_intent_id STRING,
    precise_intent_standard_q STRING,
    employee_name STRING,
    question_type INT,
    shop_name STRING,
    cond_answer_id STRING
)
PARTITIONED BY (day INT)
STORED AS PARQUET 
LOCATION 'hdfs://znzjk-134218-test-mini-bigdata-clickhouse:8020/user/hive/warehouse/test.db/mini_xdrs_log'

-- 创建聊天测试表2
CREATE TABLE test.mini_xdrs_log_1 (
    snick STRING,
    shop_id STRING,
    cnick STRING,
    msg STRING,
    act STRING,
    mode STRING,
    msg_time BIGINT,
    category STRING,
    platform STRING,
    mp_version STRING,
    qa_id STRING,
    is_identified INT,
    plat_goods_id STRING,
    shop_question_type STRING,
    shop_question_id STRING,
    question_b_qid STRING,
    question_b_proba DOUBLE,
    question_b_standard_q STRING,
    intent STRING,
    current_sale_stage STRING,
    is_robot_answer INT,
    create_time STRING,
    answer_id STRING,
    answer_explain STRING,
    question_b_id STRING,
    remind_answer STRING,
    msg_id STRING,
    task_id STRING,
    answer_source STRING,
    send_msg_from STRING,
    nick STRING,
    checked_order_mark BOOLEAN,
    is_precise_intent BOOLEAN,
    precise_intent_id STRING,
    precise_intent_standard_q STRING,
    employee_name STRING,
    question_type INT,
    shop_name STRING,
    cond_answer_id STRING
)
PARTITIONED BY (day INT)
STORED AS PARQUET 
LOCATION 'hdfs://znzjk-134218-test-mini-bigdata-clickhouse:8020/user/hive/warehouse/test.db/mini_xdrs_log_1'

-- 修改测试表1的表属性,增加排序列
ALTER TABLE test.mini_xdrs_log SET TBLPROPERTIES('sort.columns' = 'shop_id, act')

-- 给测试表1插入数据
INSERT INTO TABLE test.mini_xdrs_log PARTITION(day=20210814)
SELECT snick,
    shop_id,
    cnick,
    msg,
    act,
    mode,
    msg_time,
    category,
    platform,
    mp_version,
    qa_id,
    is_identified,
    plat_goods_id,
    shop_question_type,
    shop_question_id,
    question_b_qid,
    question_b_proba,
    question_b_standard_q,
    intent,
    current_sale_stage,
    is_robot_answer,
    create_time,
    answer_id,
    answer_explain,
    question_b_id,
    remind_answer,
    msg_id,
    task_id,
    answer_source,
    send_msg_from,
    nick,
    checked_order_mark,
    is_precise_intent,
    precise_intent_id,
    precise_intent_standard_q,
    employee_name,
    question_type,
    shop_name,
    cond_answer_id
FROM dwd.mini_xdrs_log
WHERE day = 20210814

-- 给测试表2插入数据
INSERT INTO TABLE test.mini_xdrs_log_1 PARTITION(day=20210814)
SELECT snick,
    shop_id,
    cnick,
    msg,
    act,
    mode,
    msg_time,
    category,
    platform,
    mp_version,
    qa_id,
    is_identified,
    plat_goods_id,
    shop_question_type,
    shop_question_id,
    question_b_qid,
    question_b_proba,
    question_b_standard_q,
    intent,
    current_sale_stage,
    is_robot_answer,
    create_time,
    answer_id,
    answer_explain,
    question_b_id,
    remind_answer,
    msg_id,
    task_id,
    answer_source,
    send_msg_from,
    nick,
    checked_order_mark,
    is_precise_intent,
    precise_intent_id,
    precise_intent_standard_q,
    employee_name,
    question_type,
    shop_name,
    cond_answer_id
FROM dwd.mini_xdrs_log
WHERE day = 20210814

-- 在测试表1和测试表2中分别执行查询测试用例, 通过CDH Impala UI对比两者的性能
-- 1. 测试用例1
SELECT snick,
    cnick,
    msg,
    act,
    msg_time,
    create_time,
    question_b_qid,
    question_b_standard_q,
    plat_goods_id
FROM dwd.mini_xdrs_log
WHERE DAY >= 20210814
    AND DAY <= 20210814
    AND send_msg_from NOT IN ('0', '1')
    AND shop_id = "5cf0d72498ef41000f114a0f"
ORDER BY cnick,
    msg_time
limit 500000 -- trace:9d54519d4bc7676fe15756f22189759b
-- 2. 测试用例2
SELECT split_part(snick, ':', 1) AS seller_nick,
    cnick,
    category,
    act,
    msg,
    remind_answer,
    cast(msg_time AS String) AS msg_time,
    question_b_qid,
    question_b_proba,
    MODE,
    DAY,
    create_time,
    is_robot_answer,
    plat_goods_id,
    current_sale_stage,
    uuid() AS sample_id
FROM dwd.mini_xdrs_log
WHERE act = 'recv_msg'
    AND platform = "tb"
    AND DAY >= 20210814
    AND DAY <= 20210814
    AND act not in ('statistics_send_msg', '')
    AND category IN ("k-887-tb")
    AND cast(question_b_qid AS INTEGER) >= 0
    AND question_b_proba > 0.900000
-- trace:ee20ac09d6b6e9a21803a3e5bf026c97
-- PS: 通过CDH Impala UI对比两者的性能