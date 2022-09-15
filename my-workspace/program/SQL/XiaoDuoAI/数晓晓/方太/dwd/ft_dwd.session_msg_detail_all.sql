CREATE DATABASE IF NOT EXISTS ft_dwd ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE ft_dwd.session_msg_detail_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_dwd.session_msg_detail_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `session_id` String,
    `snick` String,
    `cnick` String,
    `real_buyer_nick` String,
    `act` String,
    `msg_id` String,
    `msg_time` DateTime,
    `msg` String,
    `plat_goods_id` String,
    `is_first_msg_within_session` UInt8
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, shop_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE ft_dwd.session_msg_detail_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_dwd.session_msg_detail_all ON CLUSTER cluster_3s_2r
AS ft_dwd.session_msg_detail_local
ENGINE = Distributed('cluster_3s_2r', 'ft_dwd', 'session_msg_detail_local', rand())

-- DROP TABLE buffer.ft_dwd_session_msg_detail_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.ft_dwd_session_msg_detail_buffer ON CLUSTER cluster_3s_2r
AS ft_dwd.session_msg_detail_all
ENGINE = Buffer('ft_dwd', 'session_msg_detail_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)

-- ETL
-- TRUNCATE TABLE ft_dwd.session_msg_detail_local ON CLUSTER cluster_3s_2r
INSERT INTO buffer.ft_dwd_session_msg_detail_buffer
SELECT
    day, platform, shop_id, shop_name,
    lower(hex(MD5(concat(toString(day), shop_id, snick, cnick, real_buyer_nick, toString(session_num))))) AS session_id,
    snick,
    cnick,
    real_buyer_nick,
    act,
    msg_id,
    msg_time,
    msg,
    plat_goods_id,
    is_first_msg_within_session
FROM (
    SELECT
        day,
        platform,
        shop_id,
        shop_info.shop_name,
        snick,
        cnick,
        real_buyer_nick,
        plat_goods_ids,
        acts,
        msg_times,
        pre_msg_times,
        msg_time_delta_mins,
        is_first_msg_within_sessions,
        session_nums,
        msgs,
        msg_ids
    FROM (
        SELECT
            day,
            platform,
            shop_id,
            snick,
            cnick,
            real_buyer_nick,
            groupArray(plat_goods_id) AS plat_goods_ids,
            groupArray(act) AS acts,
            groupArray(msg_time) AS msg_times,
            arrayPushFront(arrayPopBack(msg_times), toDateTime(1)) AS pre_msg_times,
            arrayMap((x,y)->( (toDateTime(x)-toDateTime(y))/60 ), msg_times, pre_msg_times) AS msg_time_delta_mins,
            arrayMap((x) -> (IF(x>10, 1, 0)), msg_time_delta_mins) AS is_first_msg_within_sessions,
            arrayMap((x,y)->(arraySum(arraySlice(is_first_msg_within_sessions, 1, y))), is_first_msg_within_sessions, arrayEnumerate(is_first_msg_within_sessions)) AS session_nums,
            groupArray(msg) AS msgs,
            groupArray(msg_id) AS msg_ids
        FROM (
            SELECT
                day,
                platform,
                shop_id,
                replaceOne(snick,'cntaobao','') AS snick,
                replaceOne(cnick,'cntaobao','') AS cnick,
                real_buyer_nick,
                plat_goods_id,
                act,
                msg_time,
                msg,
                msg_id
            FROM ft_ods.xdrs_logs_all
            WHERE day BETWEEN 20220801 AND 20220811
            AND act IN ['send_msg', 'recv_msg']
            ORDER BY day, platform, shop_id, snick, cnick, real_buyer_nick, msg_time
        ) AS message_info
        GROUP BY day, platform, shop_id, snick, cnick, real_buyer_nick
    ) AS session_info
    GLOBAL LEFT JOIN (
        SELECT
            'tb' AS platform,
            '5cac112e98ef4100118a9c9f' AS shop_id,
            '方太官方旗舰店' AS shop_name
        FROM numbers(1)
    ) AS shop_info
    USING(shop_id)
) AS session_detail_info
ARRAY JOIN
    plat_goods_ids AS plat_goods_id,
    acts AS act,
    msg_times AS msg_time,
    pre_msg_times AS pre_msg_time,
    msg_time_delta_mins AS msg_time_delta_min,
    is_first_msg_within_sessions AS is_first_msg_within_session,
    session_nums AS session_num,
    msgs AS msg,
    msg_ids AS msg_id
