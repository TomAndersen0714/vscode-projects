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
    `reply_question_cnt` Int64,
    `m_reply_question_cnt` Int64,
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


INSERT INTO {sink_table}
SELECT
    day,
    platform,
    shop_id,
    shop_name,
    snick,
    uniqExact(cnick) AS recv_cnick_cnt,
    uniqExactIf(cnick, session_send_cnt>0) AS reply_cnick_cnt,
    uniqExactIf(cnick, m_session_send_cnt>0) AS m_reply_cnick_cnt,
    uniqExact(session_id) AS session_cnt,
    uniqExactIf(session_id, qa_reply_intervals_secs[1]<30) AS first_reply_within_thirty_secs_session_cnt,
    uniqExactIf(session_id, m_qa_reply_intervals_secs[1]<30) AS m_first_reply_within_thirty_secs_session_cnt,
    SUM(session_recv_cnt) AS recv_msg_cnt,
    SUM(session_send_cnt) AS send_msg_cnt,
    SUM(m_session_send_cnt) AS m_send_msg_cnt,
    SUM(qa_cnt) AS reply_question_cnt,
    SUM(m_qa_cnt) AS m_reply_question_cnt,
    SUM(qa_reply_intervals_secs[1]) AS reply_interval_secs_sum,
    SUM(m_qa_reply_intervals_secs[1]) AS m_reply_interval_secs_sum
FROM ft_dwd.session_detail_all
WHERE day = {ds_nodash}
AND platform = '{platform}'
AND shop_id = '{shop_id}'
GROUP BY day, platform, shop_id, shop_name, snick