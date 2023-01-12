-- 方太会话粒度统计, 以及关联转接记录
ALTER TABLE ft_dwd.session_detail_local ON CLUSTER cluster_3s_2r DROP PARTITION ({{ds_nodash}}, '{{platform}}');

SELECT sleep(3);

-- 1. 方太会话关联转出记录标签
-- 转出记录必须在会话结束10分钟之内, 即下一次切割时间点之前会话结束之后
-- 多条转接记录, 匹配上同一条会话时, 仅取最新的转接记录
INSERT INTO ft_dwd.session_detail_all
SELECT
    day, platform, shop_id, shop_name,
    session_id, snick, cnick, real_buyer_nick,
    focus_goods_ids,
    arrayDistinct(c_active_send_goods_ids) AS c_active_send_goods_ids,
    arrayDistinct(s_active_send_goods_ids) AS s_active_send_goods_ids,
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
        groupArray(plat_goods_id) AS _focus_goods_ids,
        groupArray(act) AS _msg_acts,
        arrayMap(
            (x,y)->(x !='' AND not has(arraySlice(_focus_goods_ids, 1, y-1), x)), 
            _focus_goods_ids, arrayEnumerate(_focus_goods_ids)
        ) AS _is_first_occurreds,
        arrayFilter((x,y,z)->(y='recv_msg' AND z=1), _focus_goods_ids, _msg_acts, _is_first_occurreds) AS c_active_send_goods_ids,
        arrayFilter((x,y,z)->(y='send_msg' AND z=1), _focus_goods_ids, _msg_acts, _is_first_occurreds) AS s_active_send_goods_ids,
        toString(min(msg_time)) AS session_start_time,
        toString(max(msg_time)) AS session_end_time,
        toString(minIf(msg_time, act='recv_msg')) AS recv_msg_start_time,
        toString(maxIf(msg_time, act='recv_msg')) AS recv_msg_end_time,
        toString(minIf(msg_time, act='send_msg')) AS send_msg_start_time,
        toString(maxIf(msg_time, act='send_msg')) AS send_msg_end_time,
        SUM(act = 'recv_msg') AS session_recv_cnt,
        SUM(act = 'send_msg') AS session_send_cnt
    FROM (
        SELECT
            day, platform, shop_id, shop_name,
            session_id, snick, cnick, real_buyer_nick,
            act, plat_goods_id, msg_time
        FROM ft_dwd.session_msg_detail_all
        WHERE day = {{ds_nodash}}
        AND platform = 'tb'
        AND shop_id = '{{shop_id}}'
        ORDER BY session_id, msg_time ASC
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
    WHERE day = {{ds_nodash}}
    AND platform = 'tb'
    AND shop_id = '{{shop_id}}'
) AS transfer_msg_info
ON session_info.day = transfer_msg_info.day
AND session_info.shop_id = transfer_msg_info.shop_id
AND session_info.snick = transfer_msg_info.from_snick
-- tb使用cnick关联
AND session_info.cnick = transfer_msg_info.cnick
WHERE toDateTime64(create_time, 0) >= toDateTime64(session_end_time, 0)
AND toDateTime64(create_time, 0) <= toDateTime64(session_end_time, 0) + 600
ORDER BY session_id, transfer_time DESC
LIMIT 1 BY session_id;


-- 等待数据写入
SELECT sleep(3);
SELECT sleep(3);
SELECT sleep(3);


-- 2. 方太会话匹配转入记录标签
-- 转入记录必须在会话开始之前10分钟之内, 即上一次切割时间点之后会话开始之前
-- 多条转接记录, 匹配上同一条会话时, 仅取最新的转接记录
INSERT INTO ft_dwd.session_detail_all
SELECT
    day, platform, shop_id, shop_name,
    session_id, snick, cnick, real_buyer_nick,
    focus_goods_ids,
    c_active_send_goods_ids,
    s_active_send_goods_ids,
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
        groupArray(plat_goods_id) AS _focus_goods_ids,
        groupArray(act) AS _msg_acts,
        arrayMap(
            (x,y)->(x !='' AND not has(arraySlice(_focus_goods_ids, 1, y-1), x)), 
            _focus_goods_ids, arrayEnumerate(_focus_goods_ids)
        ) AS _is_first_occurreds,
        arrayFilter((x,y,z)->(y='recv_msg' AND z=1), _focus_goods_ids, _msg_acts, _is_first_occurreds) AS c_active_send_goods_ids,
        arrayFilter((x,y,z)->(y='send_msg' AND z=1), _focus_goods_ids, _msg_acts, _is_first_occurreds) AS s_active_send_goods_ids,
        toString(min(msg_time)) AS session_start_time,
        toString(max(msg_time)) AS session_end_time,
        toString(minIf(msg_time, act='recv_msg')) AS recv_msg_start_time,
        toString(maxIf(msg_time, act='recv_msg')) AS recv_msg_end_time,
        toString(minIf(msg_time, act='send_msg')) AS send_msg_start_time,
        toString(maxIf(msg_time, act='send_msg')) AS send_msg_end_time,
        SUM(act = 'recv_msg') AS session_recv_cnt,
        SUM(act = 'send_msg') AS session_send_cnt
    FROM (
        SELECT
            day, platform, shop_id, shop_name,
            session_id, snick, cnick, real_buyer_nick,
            act, plat_goods_id, msg_time
        FROM ft_dwd.session_msg_detail_all
        WHERE day = {{ds_nodash}}
        AND platform = 'tb'
        AND shop_id = '{{shop_id}}'
        AND session_id GLOBAL NOT IN (
            SELECT DISTINCT
                session_id
            FROM ft_dwd.session_detail_all
            WHERE day = {{ds_nodash}}
            AND platform = 'tb'
            AND shop_id = '{{shop_id}}'
        )
        ORDER BY session_id, msg_time ASC
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
    WHERE day = {{ds_nodash}}
    AND platform = 'tb'
    AND shop_id = '{{shop_id}}'
) AS transfer_msg_info
ON session_info.day = transfer_msg_info.day
AND session_info.shop_id = transfer_msg_info.shop_id
AND session_info.snick = transfer_msg_info.to_snick
-- tb使用cnick关联
AND session_info.cnick = transfer_msg_info.cnick
WHERE toDateTime64(create_time, 0) <= toDateTime64(session_start_time, 0)
AND toDateTime64(create_time, 0) >= toDateTime64(session_start_time, 0) - 600
ORDER BY session_id, transfer_time DESC
LIMIT 1 BY session_id;


-- 等待数据写入
SELECT sleep(3);
SELECT sleep(3);
SELECT sleep(3);


-- 3. 针对未匹配上转接记录的会话统计
INSERT INTO ft_dwd.session_detail_all
SELECT
    day, platform, shop_id, shop_name,
    session_id, snick, cnick, real_buyer_nick,
    focus_goods_ids,
    c_active_send_goods_ids,
    s_active_send_goods_ids,
    session_start_time,
    session_end_time,
    recv_msg_start_time,
    recv_msg_end_time,
    send_msg_start_time,
    send_msg_end_time,
    session_recv_cnt,
    session_send_cnt,
    has_transfer,
    transfer_id,
    transfer_from_snick,
    transfer_to_snick,
    transfer_time
FROM (
    SELECT
        day, platform, shop_id, shop_name,
        session_id, snick, cnick, real_buyer_nick,
        groupUniqArrayIf(plat_goods_id, plat_goods_id!='') AS focus_goods_ids,
        groupArray(plat_goods_id) AS _focus_goods_ids,
        groupArray(act) AS _msg_acts,
        arrayMap(
            (x,y)->(x !='' AND not has(arraySlice(_focus_goods_ids, 1, y-1), x)), 
            _focus_goods_ids, arrayEnumerate(_focus_goods_ids)
        ) AS _is_first_occurreds,
        arrayFilter((x,y,z)->(y='recv_msg' AND z=1), _focus_goods_ids, _msg_acts, _is_first_occurreds) AS c_active_send_goods_ids,
        arrayFilter((x,y,z)->(y='send_msg' AND z=1), _focus_goods_ids, _msg_acts, _is_first_occurreds) AS s_active_send_goods_ids,
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
    FROM (
        SELECT
            day, platform, shop_id, shop_name,
            session_id, snick, cnick, real_buyer_nick,
            act, plat_goods_id, msg_time
        FROM ft_dwd.session_msg_detail_all
        WHERE day = {{ds_nodash}}
        AND platform = 'tb'
        AND shop_id = '{{shop_id}}'
        AND session_id GLOBAL NOT IN (
            SELECT DISTINCT
                session_id
            FROM ft_dwd.session_detail_all
            WHERE day = {{ds_nodash}}
            AND platform = 'tb'
            AND shop_id = '{{shop_id}}'
        )
        ORDER BY session_id, msg_time ASC
    )
    GROUP BY day, platform, shop_id, shop_name, session_id, snick, cnick, real_buyer_nick
)

-- 等待数据写入
SELECT sleep(3);