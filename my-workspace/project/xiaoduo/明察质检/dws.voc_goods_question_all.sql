CREATE DATABASE IF NOT EXISTS dws ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE dws.voc_goods_question_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dws.voc_goods_question_local ON CLUSTER cluster_3s_2r
(
    `day` UInt32, 
    `platform` String,
    `shop_id` String,
    `snick` String,
    `question_id` String,
    `dialog_qa_stage` UInt64,
    `dialog_goods_id` String,
    `recent_order_id` String,
    `recent_order_status` String,
    `uid_bitmap` AggregateFunction(groupBitmap, UInt64),
    `dialog_sum` UInt64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, shop_id, snick)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE dws.voc_goods_question_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dws.voc_goods_question_all ON CLUSTER cluster_3s_2r
AS dws.voc_goods_question_local
ENGINE = Distributed('cluster_3s_2r', 'dws', 'voc_goods_question_local', rand());


CREATE DATABASE IF NOT EXISTS buffer ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE buffer.dws_voc_goods_question_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS buffer.dws_voc_goods_question_buffer ON CLUSTER cluster_3s_2r
AS dws.voc_goods_question_all
ENGINE = Buffer('dws', 'voc_goods_question_all', 16, 15, 35, 81920, 409600, 16777216, 67108864);