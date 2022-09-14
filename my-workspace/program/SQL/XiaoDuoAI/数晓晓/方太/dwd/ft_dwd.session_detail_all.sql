CREATE DATABASE IF NOT EXISTS ft_dwd ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE ft_dwd.session_detail_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_dwd.session_detail_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `shop_id` String,
    `session_id` String,
    `snick` String,
    `cnick` String,
    `real_buyer_nick` String,
    `focus_goods_ids` Array(String),
    `session_start_time` String,
    `session_end_time` String,
    `recv_msg_start_time` String,
    `recv_msg_end_time` String,
    `send_msg_start_time` String,
    `send_msg_end_time` String,
    `session_recv_cnt` Int64,
    `session_send_cnt` Int64,
    `has_transfer` Int8,
    `transfer_from_snick` String,
    `transfer_to_snick` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, shop_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE ft_dwd.session_detail_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_dwd.session_detail_all ON CLUSTER cluster_3s_2r
AS ft_dwd.session_detail_local
ENGINE = Distributed('cluster_3s_2r', 'ft_dwd', 'session_detail_local', rand())

-- DROP TABLE buffer.ft_dwd_session_detail_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.ft_dwd_session_detail_buffer ON CLUSTER cluster_3s_2r
AS ft_dwd.session_detail_all
ENGINE = Buffer('ft_dwd', 'session_detail_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)

-- INSERT INTO
-- TRUNCATE TABLE ft_dwd.session_detail_local ON CLUSTER cluster_3s_2r NO DELAY
INSERT INTO buffer.ft_dwd_session_detail_buffer
SELECT
    day, platform, shop_id,
    lower(hex(MD5(concat(cnick, snick, real_buyer_nick, toString(min(msg_time)))))) AS session_id,
    snick, cnick, real_buyer_nick,
    groupUniqArrayIf(plat_goods_id, plat_goods_id!='') AS focus_goods_ids,
    toString(min(msg_time)) AS session_start_time,
    toString(max(msg_time)) AS session_end_time,
    toString(minIf(msg_time, act='recv_msg')) AS recv_msg_start_time,
    toString(maxIf(msg_time, act='recv_msg')) AS recv_msg_end_time,
    toString(minIf(msg_time, act='send_msg')) AS send_msg_start_time,
    toString(maxIf(msg_time, act='send_msg')) AS send_msg_end_time,
    SUM(act = 'recv_msg') AS session_recv_cnt,
    SUM(act = 'send_msg') AS session_send_cnt,
    0 AS has_transfer,
    '' AS transfer_from_snick,
    '' AS transfer_to_snick
FROM ft_dwd.session_msg_detail_all
WHERE day BETWEEN 20220801 AND 20220910
GROUP BY day, platform, shop_id, snick, cnick, real_buyer_nick