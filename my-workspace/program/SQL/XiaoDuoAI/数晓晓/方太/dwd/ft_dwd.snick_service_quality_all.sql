CREATE DATABASE IF NOT EXISTS ft_dwd ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE ft_dwd.snick_service_quality_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_dwd.snick_service_quality_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `snick` String,
    `recv_cnick_cnt` Int64,
    `reply_cnick_cnt` Int64,
    `m_reply_cnick_cnt` Int64,
    `session_cnt` Int64,
    `first_reply_within_thirty_secs_session_cnt` Int64,
    `m_first_reply_within_thirty_secs_session_cnt` Int64,
    `recv_msg_cnt` Int64,
    `send_msg_cnt` Int64,
    `m_send_msg_cnt` Int64,
    `reply_cnt` Int64,
    `m_reply_cnt` Int64,
    `reply_interval_secs_sum` Int32,
    `m_reply_interval_secs_sum` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (shop_id, snick)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE ft_dwd.snick_service_quality_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_dwd.snick_service_quality_all ON CLUSTER cluster_3s_2r
AS ft_dwd.snick_service_quality_local
ENGINE = Distributed('cluster_3s_2r', 'ft_dwd', 'snick_service_quality_local', rand())

-- DROP TABLE buffer.ft_dwd_snick_service_quality_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.ft_dwd_snick_service_quality_buffer ON CLUSTER cluster_3s_2r
AS ft_dwd.snick_service_quality_all
ENGINE = Buffer('ft_dwd', 'snick_service_quality_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)