-- ft_dim
CREATE DATABASE IF NOT EXISTS ft_dim ON CLUSTER cluster_3s_2r ENGINE = Ordinary;


-- ft_dim.account_filter_all
-- DROP TABLE ft_dim.account_filter_1_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS ft_dim.account_filter_1_local ON CLUSTER cluster_3s_2r
(
    `platform` String,
    `shop_id` String,
    `cnick` String,
    `acc_type` String,
    `reason` String,
    `update_time` String,
    `attr` String,
    `raw` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (update_time, acc_type)
SETTINGS storage_policy = 'rr', index_granularity = 8192;


-- DROP TABLE ft_dim.account_filter_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS ft_dim.account_filter_all ON CLUSTER cluster_3s_2r
AS ft_dim.account_filter_1_local
ENGINE = Distributed('cluster_3s_2r', 'ft_dim', 'account_filter_1_local', rand());


-- ft_dim.snick_info_all
-- DROP TABLE ft_dim.snick_info_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS ft_dim.snick_info_local ON CLUSTER cluster_3s_2r
(
    `platform` String,
    `shop_id` String,
    `snick` String,
    `team` String,
    `name` String,
    `raw` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY shop_id
SETTINGS storage_policy = 'rr', index_granularity = 8192;


-- DROP TABLE ft_dim.snick_info_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS ft_dim.snick_info_all ON CLUSTER cluster_3s_2r
AS ft_dim.snick_info_local
ENGINE = Distributed('cluster_3s_2r', 'ft_dim', 'snick_info_local', rand());


-- DROP TABLE ft_dim.goods_info_1_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS ft_dim.goods_info_1_local  ON CLUSTER cluster_3s_2r
(
    `platform` String,
    `shop_id` String,
    `goods_id` String,
    `title` String,
    `cate_lv1` String,
    `cate_lv2` String,
    `cate_lv3` String,
    `enable` String,
    `goods_url` String,
    `price` String,
    `type` String,
    `attr` String,
    `raw` String
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (type, goods_id)
SETTINGS storage_policy = 'rr', index_granularity = 8192;

-- DROP TABLE ft_dim.goods_info_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS ft_dim.goods_info_all ON CLUSTER cluster_3s_2r
AS ft_dim.goods_info_1_local
ENGINE = Distributed('cluster_3s_2r', 'ft_dim', 'goods_info_1_local', rand());


-- ft_dwd.session_msg_detail_all
-- ALTER TABLE ft_dwd.session_msg_detail_local ON CLUSTER cluster_3s_2r DROP COLUMN IF EXISTS `send_msg_from`
ALTER TABLE ft_dwd.session_msg_detail_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `send_msg_from` Int64 AFTER `is_first_msg_within_session`;

-- ALTER TABLE ft_dwd.session_msg_detail_all ON CLUSTER cluster_3s_2r DROP COLUMN IF EXISTS `send_msg_from`
ALTER TABLE ft_dwd.session_msg_detail_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `send_msg_from` Int64 AFTER `is_first_msg_within_session`;

-- ft_dwd.session_detail_all
ALTER TABLE ft_dwd.session_detail_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `m_session_send_cnt` Int64 AFTER `session_send_cnt`,
ADD COLUMN IF NOT EXISTS `qa_cnt` Int64 AFTER `m_session_send_cnt`,
ADD COLUMN IF NOT EXISTS `qa_reply_intervals_secs` Array(Int64) AFTER `qa_cnt`,
ADD COLUMN IF NOT EXISTS `m_qa_cnt` Int64 AFTER `qa_reply_intervals_secs`,
ADD COLUMN IF NOT EXISTS `m_qa_reply_intervals_secs` Array(Int64) AFTER `m_qa_cnt`;

ALTER TABLE ft_dwd.session_detail_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `m_session_send_cnt` Int64 AFTER `session_send_cnt`,
ADD COLUMN IF NOT EXISTS `qa_cnt` Int64 AFTER `m_session_send_cnt`,
ADD COLUMN IF NOT EXISTS `qa_reply_intervals_secs` Array(Int64) AFTER `qa_cnt`,
ADD COLUMN IF NOT EXISTS `m_qa_cnt` Int64 AFTER `qa_reply_intervals_secs`,
ADD COLUMN IF NOT EXISTS `m_qa_reply_intervals_secs` Array(Int64) AFTER `m_qa_cnt`;

ALTER TABLE buffer.ft_dwd_session_detail_buffer ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `m_session_send_cnt` Int64 AFTER `session_send_cnt`,
ADD COLUMN IF NOT EXISTS `qa_cnt` Int64 AFTER `m_session_send_cnt`,
ADD COLUMN IF NOT EXISTS `qa_reply_intervals_secs` Array(Int64) AFTER `qa_cnt`,
ADD COLUMN IF NOT EXISTS `m_qa_cnt` Int64 AFTER `qa_reply_intervals_secs`,
ADD COLUMN IF NOT EXISTS `m_qa_reply_intervals_secs` Array(Int64) AFTER `m_qa_cnt`;