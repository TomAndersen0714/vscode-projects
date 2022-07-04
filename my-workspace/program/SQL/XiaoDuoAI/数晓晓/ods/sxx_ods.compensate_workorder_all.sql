CREATE DATABASE sxx_ods ON CLUSTER cluster_3s_2r

-- DROP TABLE sxx_ods.compensate_workorder_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_ods.compensate_workorder_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `platform_cn` String,
    `shop_id` String,
    `shop_name` String,
    `raw_info` String,
    `category` String,
    `type` String,
    `id` String,
    `status` String,
    `handler` String,
    `priority` String,
    `create_time` String,
    `creator` String,
    `description` String,
    `finish_time` String,
    `eval_status` String,
    `score` String,
    `custom_opinion` String,
    `order_id` String,
    `money` Int64,
    `order_status` String,
    `order_goods` String,
    `custom_goods` String,
    `delivery_id` String,
    `paid_time` String,
    `product_time` String,
    `logistics_expire_limit` String,
    `reason_full` String,
    `reason_level_1` String,
    `reason_level_2` String,
    `reason_level_3` String,
    `reason_level_4` String,
    `compensate_cnt` Int64,
    `unit_name` String,
    `compensate_type` String,
    `transfer_money` Int64,
    `relative_pic` String,
    `factory_code` String,
    `is_delivery_paid` String,
    `is_special` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (day, platform, order_id, reason_level_3, reason_level_4)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE sxx_ods.compensate_workorder_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_ods.compensate_workorder_all ON CLUSTER cluster_3s_2r
AS sxx_ods.compensate_workorder_local
ENGINE = Distributed('cluster_3s_2r', 'sxx_ods', 'compensate_workorder_local', rand())

-- DROP TABLE buffer.sxx_ods_compensate_workorder_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.sxx_ods_compensate_workorder_buffer ON CLUSTER cluster_3s_2r
AS sxx_ods.compensate_workorder_all
ENGINE = Buffer('sxx_ods', 'compensate_workorder_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)

