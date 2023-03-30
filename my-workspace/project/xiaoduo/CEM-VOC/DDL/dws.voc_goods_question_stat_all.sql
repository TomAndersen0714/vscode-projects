CREATE DATABASE IF NOT EXISTS dws ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE dws.voc_goods_question_stat_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dws.voc_goods_question_stat_local ON CLUSTER cluster_3s_2r
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
    `recent_order_status_timestamp` UInt64,
    `cnick_id_bitmap` AggregateFunction(groupBitmap, UInt64),
    `dialog_sum` UInt64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, shop_id, snick)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE dws.voc_goods_question_stat_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dws.voc_goods_question_stat_all ON CLUSTER cluster_3s_2r
AS dws.voc_goods_question_stat_local
ENGINE = Distributed('cluster_3s_2r', 'dws', 'voc_goods_question_stat_local', rand());
