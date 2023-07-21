-- DROP TABLE ods.voc_customer_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS ods.voc_customer_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` Int64,
    `update_time` Int64,
    `company_id` String,
    `shop_id` String,
    `platform` String,
    `seller_nick` String,
    `cnick` String,
    `real_buyer_nick` String,
    `reception_time` Int64,
    `tags` Array(Int64),
    `order_status` Int64,
    `dialog_info_dialog_id` Array(String),
    `dialog_info_begin_time` Array(Int64),
    `dialog_info_goods_id` Array(String),
    `dialog_info_snick` Array(String),
    `day` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (company_id, shop_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE ods.voc_customer_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS ods.voc_customer_all ON CLUSTER cluster_3s_2r
AS ods.voc_customer_local
ENGINE = Distributed('cluster_3s_2r', 'ods', 'voc_customer_local', rand());