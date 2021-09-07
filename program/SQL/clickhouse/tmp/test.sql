
SELECT 
    question, question_id, qoid, pv
FROM (
    SELECT qid, question
    FROM dim.question_b
) AS dim
RIGHT JOIN(
    SELECT 
        question_id, qoid, pv
    FROM 
        app_mp.stat_question_for_shop
    WHERE 
        day = CAST(replace('{{ day }}','-','') AS INT)
        AND snick_oid = '{{ shop_id }}'
    ORDER BY pv DESC
    LIMIT 20
) AS ods
ON dim.qid = ods.qoid


DROP TABLE tmp.test_tbl_1_local ON CLUSTER cluster_3s_2r
CREATE TABLE tmp.test_tbl_1_local ON CLUSTER cluster_3s_2r
AS tmp.test_tbl_local
ENGINE = ReplicatedMergeTree('/clickhouse/tmp/tables/{layer}_{shard}/test_tbl_1_local', '{replica}')
PARTITION BY version
ORDER BY (_id, version)
SETTINGS index_granularity = 8192, storage_policy = 'default'

DROP TABLE tmp.test_tbl_1_all ON CLUSTER cluster_3s_2r
CREATE TABLE tmp.test_tbl_1_all ON CLUSTER cluster_3s_2r
AS tmp.test_tbl_1_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'test_tbl_1_local', rand())


TRUNCATE TABLE tmp.test_tbl_1_local ON CLUSTER cluster_3s_2r
OPTIMIZE TABLE tmp.test_tbl_1_local ON CLUSTER cluster_3s_2r

CREATE TABLE test.biz_log 
(
    `ts` DateTime, 
    `tag` String, 
    `message` String
) 
ENGINE = Kafka 
SETTINGS kafka_broker_list = '10.0.2.0:9092,10.0.2.1:9092,10.0.2.2:9092',
kafka_topic_list = 'tag',
kafka_group_name = 'bigdata',
kafka_format = 'JSONEachRow',
kafka_skip_broken_messages = 1,
kafka_num_consumers = 2


-- CREATE DATABASE dipper ON cluster cluster_3s_2r
CREATE TABLE dipper.ask_order_conversion_stat_day_local ON cluster cluster_3s_2r( 
    `day` Int32,
    `shop_id` String,  
    `platform` String, 
    `ao_category` String,  
    `ao_total_consult_order_cuv` Int64, 
    `ao_ordered_cuv` Int64, 
    `ao_paid_cuv` Int64, 
    `ao_ordered_volume` Int64, 
    `ao_sold_money_volume` Float32, 
    `ao_avg_transaction_value` Float32
)
ENGINE = ReplicatedMergeTree('/clickhouse/dipper/tables/{layer}_{shard}/ask_order_conversion_stat_day_local', '{replica}')  
PARTITION BY day 
ORDER BY (shop_id,platform) SETTINGS index_granularity = 8192

CREATE TABLE dipper.ask_order_conversion_stat_day_all on cluster cluster_3s_2r
as dipper.ask_order_conversion_stat_day_local
ENGINE = Distributed('cluster_3s_2r', 'dipper', 'ask_order_conversion_stat_day_local', rand())


SELECT distinct
    _id,
    if((neighbor(_id,-1,'') == _id),neighbor(version,-1),version)
FROM (
    SELECT *
    FROM tmp.test_tbl_all
    ORDER BY _id,version DESC
) AS t1



CREATE TABLE alerts_amt_max (
    tenant_id UInt32,
    alert_id String,
    timestamp DateTime Codec(Delta, LZ4),
    alert_data SimpleAggregateFunction(max, String),
    acked SimpleAggregateFunction(max, UInt8),
    ack_time SimpleAggregateFunction(max, DateTime),
    ack_user SimpleAggregateFunction(max, LowCardinality(String))
) Engine = AggregatingMergeTree()
ORDER BY (tenant_id, timestamp, alert_id);

CREATE TABLE xqc_ods.xdqc_tb_task_record_back_local
AS xqc_ods.xdqc_tb_task_record_local
ENGINE = MergeTree()
ORDER BY mp_shop_id
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_ods.xdqc_tb_task_record_local ON CLUSTER cluster_3s_2r
CREATE TABLE xqc_ods.xdqc_tb_task_record_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `platform` String,
    `channel` String,
    `seller_nick` String,
    `group` String,
    `date` Int64,
    `account_name` String,
    `account_id` String,
    `task_mode` Int64,
    `dialog_count` Int64,
    `abnormal_dialog_count` Int64,
    `mark_dialog_count` Int64,
    `mp_shop_id` String
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/xqc_ods/tables/{layer}_{shard}/xdqc_tb_task_record_local',
    '{replica}'
) 
PARTITION BY (`date`, platform)
ORDER BY (mp_shop_id, account_id) 
SETTINGS index_granularity = 8192, storage_policy = 'rr'

-- DROP TABLE xqc_ods.xdqc_tb_task_record_all ON CLUSTER cluster_3s_2r
CREATE TABLE xqc_ods.xdqc_tb_task_record_all ON CLUSTER cluster_3s_2r
AS xqc_ods.xdqc_tb_task_record_local
ENGINE = Distributed('cluster_3s_2r','xqc_ods','xdqc_tb_task_record_local',rand())


INSERT INTO xqc_ods.xdqc_tb_task_record_all
SELECT * FROM xqc_ods.xdqc_tb_task_record_back_local WHERE `date` <=20210815

INSERT INTO xqc_ods.xdqc_tb_task_record_all
SELECT * FROM xqc_ods.xdqc_tb_task_record_back_local WHERE `date` >20210815



WITH 1 AS num_1, 2 AS num_2
SELECT num_1,num_2

WITH (SELECT 1) AS num_1, (SELECT 2) AS num_2
SELECT num_1,num_2