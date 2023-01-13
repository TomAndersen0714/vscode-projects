CREATE DATABASE IF NOT EXISTS ft_tmp ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE ft_tmp.session_detail_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_tmp.session_detail_local ON CLUSTER cluster_3s_2r
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
PARTITION BY day
ORDER BY (platform, shop_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE ft_tmp.session_detail_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_tmp.session_detail_all ON CLUSTER cluster_3s_2r
AS ft_tmp.session_detail_local
ENGINE = Distributed('cluster_3s_2r', 'ft_tmp', 'session_detail_local', rand())

-- DROP TABLE buffer.ft_tmp_session_detail_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.ft_tmp_session_detail_buffer ON CLUSTER cluster_3s_2r
AS ft_tmp.session_detail_all
ENGINE = Buffer('ft_tmp', 'session_detail_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)

-- ETL(tb)
-- TRUNCATE TABLE ft_dwd.session_detail_local ON CLUSTER cluster_3s_2r NO DELAY
-- INSERT INTO
-- TRUNCATE TABLE ft_tmp.session_detail_local ON CLUSTER cluster_3s_2r NO DELAY
-- 会话匹配转出记录标签
-- 转出记录必须在会话结束10分钟之内, 即下一次切割时间点之前会话结束之后
-- 多条转接记录, 匹配上同一条会话时, 仅取最新的转接记录
INSERT INTO ft_tmp.session_detail_all
SELECT
    day, platform, shop_id, shop_name,
    session_id, snick, cnick, real_buyer_nick,
    focus_goods_ids,
    session_start_time,
    session_end_time,
    recv_msg_start_time,
    recv_msg_end_time,
    send_msg_start_time,
    send_msg_end_time,
    session_recv_cnt,
    session_send_cnt,
    2 AS has_transfer,
    transfer_msg_info.id AS transfer_id,
    transfer_msg_info.from_snick AS transfer_from_snick,
    transfer_msg_info.to_snick AS transfer_to_snick,
    create_time AS transfer_time
FROM (
    SELECT
        day, platform, shop_id, shop_name,
        session_id, snick, cnick, real_buyer_nick,
        groupUniqArrayIf(plat_goods_id, plat_goods_id!='') AS focus_goods_ids,
        toString(min(msg_time)) AS session_start_time,
        toString(max(msg_time)) AS session_end_time,
        toString(minIf(msg_time, act='recv_msg')) AS recv_msg_start_time,
        toString(maxIf(msg_time, act='recv_msg')) AS recv_msg_end_time,
        toString(minIf(msg_time, act='send_msg')) AS send_msg_start_time,
        toString(maxIf(msg_time, act='send_msg')) AS send_msg_end_time,
        SUM(act = 'recv_msg') AS session_recv_cnt,
        SUM(act = 'send_msg') AS session_send_cnt
    FROM ft_dwd.session_msg_detail_all
    WHERE day BETWEEN 20220801 AND 20220810
    AND platform = 'jd'
    GROUP BY day, platform, shop_id, shop_name, session_id, snick, cnick, real_buyer_nick
) AS session_info
GLOBAL INNER JOIN (
    SELECT
        id,
        day,
        platform,
        shop_id,
        from_snick,
        to_snick,
        cnick,
        real_buyer_nick,
        create_time
    FROM ft_dwd.transfer_msg_all
    WHERE day BETWEEN 20220801 AND 20220810
    AND platform = 'jd'
) AS transfer_msg_info
ON session_info.day = transfer_msg_info.day
AND session_info.shop_id = transfer_msg_info.shop_id
AND session_info.snick = transfer_msg_info.from_snick
-- tb使用cnick关联
-- AND session_info.cnick = transfer_msg_info.cnick
-- jd使用real_buyer_nick关联
AND session_info.cnick = transfer_msg_info.real_buyer_nick
WHERE toDateTime64(create_time, 0) >= toDateTime64(session_end_time, 0)
AND toDateTime64(create_time, 0) <= toDateTime64(session_end_time, 0) + 600
ORDER BY session_id, transfer_time DESC
LIMIT 1 BY session_id

-- 会话匹配转入记录标签
-- 转入记录必须在会话开始之前10分钟之内, 即上一次切割时间点之后会话开始之前
-- 多条转接记录, 匹配上同一条会话时, 仅取最新的转接记录
INSERT INTO ft_tmp.session_detail_all
SELECT
    day, platform, shop_id, shop_name,
    session_id, snick, cnick, real_buyer_nick,
    focus_goods_ids,
    session_start_time,
    session_end_time,
    recv_msg_start_time,
    recv_msg_end_time,
    send_msg_start_time,
    send_msg_end_time,
    session_recv_cnt,
    session_send_cnt,
    1 AS has_transfer,
    transfer_msg_info.id AS transfer_id,
    transfer_msg_info.from_snick AS transfer_from_snick,
    transfer_msg_info.to_snick AS transfer_to_snick,
    create_time AS transfer_time
FROM (
    SELECT
        day, platform, shop_id, shop_name,
        session_id, snick, cnick, real_buyer_nick,
        groupUniqArrayIf(plat_goods_id, plat_goods_id!='') AS focus_goods_ids,
        toString(min(msg_time)) AS session_start_time,
        toString(max(msg_time)) AS session_end_time,
        toString(minIf(msg_time, act='recv_msg')) AS recv_msg_start_time,
        toString(maxIf(msg_time, act='recv_msg')) AS recv_msg_end_time,
        toString(minIf(msg_time, act='send_msg')) AS send_msg_start_time,
        toString(maxIf(msg_time, act='send_msg')) AS send_msg_end_time,
        SUM(act = 'recv_msg') AS session_recv_cnt,
        SUM(act = 'send_msg') AS session_send_cnt
    FROM ft_dwd.session_msg_detail_all
    WHERE day BETWEEN 20220801 AND 20220810
    AND platform = 'jd'
    AND session_id GLOBAL NOT IN (
        SELECT DISTINCT
            session_id
        FROM ft_tmp.session_detail_all
        WHERE day BETWEEN 20220801 AND 20220810
        AND platform = 'jd'
    )
    GROUP BY day, platform, shop_id, shop_name, session_id, snick, cnick, real_buyer_nick
) AS session_info
GLOBAL INNER JOIN (
    SELECT
        id,
        day,
        platform,
        shop_id,
        from_snick,
        to_snick,
        cnick,
        real_buyer_nick,
        create_time
    FROM ft_dwd.transfer_msg_all
    WHERE day BETWEEN 20220801 AND 20220810
    AND platform = 'jd'
) AS transfer_msg_info
ON session_info.day = transfer_msg_info.day
AND session_info.shop_id = transfer_msg_info.shop_id
AND session_info.snick = transfer_msg_info.to_snick
-- tb使用cnick关联
-- AND session_info.cnick = transfer_msg_info.cnick
-- jd使用real_buyer_nick关联
AND session_info.cnick = transfer_msg_info.real_buyer_nick
WHERE toDateTime64(create_time, 0) <= toDateTime64(session_start_time, 0)
AND toDateTime64(create_time, 0) >= toDateTime64(session_start_time, 0) - 600
ORDER BY session_id, transfer_time DESC
LIMIT 1 BY session_id

-- 未匹配上转接记录的会话
INSERT INTO ft_tmp.session_detail_all
SELECT
    day, platform, shop_id, shop_name,
    session_id, snick, cnick, real_buyer_nick,
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
    '' AS transfer_id,
    '' AS transfer_from_snick,
    '' AS transfer_to_snick,
    '' AS transfer_time
FROM ft_dwd.session_msg_detail_all
WHERE day BETWEEN 20220801 AND 20220810
AND session_id GLOBAL NOT IN (
    SELECT DISTINCT
        session_id
    FROM ft_tmp.session_detail_all
    WHERE day BETWEEN 20220801 AND 20220810
    AND platform = 'jd'
)
GROUP BY day, platform, shop_id, shop_name, session_id, snick, cnick, real_buyer_nick

