CREATE DATABASE IF NOT EXISTS ft_dwd ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE ft_dwd.session_detail_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_dwd.session_detail_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `session_id` String,
    `snick` String,
    `cnick` String,
    `real_buyer_nick` String,
    `focus_goods_ids` Array(String),
    `c_active_send_goods_ids` Array(String),
    `s_active_send_goods_ids` Array(String),
    `session_start_time` String,
    `session_end_time` String,
    `recv_msg_start_time` String,
    `recv_msg_end_time` String,
    `send_msg_start_time` String,
    `send_msg_end_time` String,
    `session_recv_cnt` Int64,
    `session_send_cnt` Int64,
    `has_transfer` Int8,
    `transfer_id` String,
    `transfer_from_snick` String,
    `transfer_to_snick` String,
    `transfer_time` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY (day, platform)
ORDER BY shop_id
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE ft_dwd.session_detail_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_dwd.session_detail_all ON CLUSTER cluster_3s_2r
AS ft_dwd.session_detail_local
ENGINE = Distributed('cluster_3s_2r', 'ft_dwd', 'session_detail_local', rand())

-- DROP TABLE buffer.ft_dwd_session_detail_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.ft_dwd_session_detail_buffer ON CLUSTER cluster_3s_2r
AS ft_dwd.session_detail_all
ENGINE = Buffer('ft_dwd', 'session_detail_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)