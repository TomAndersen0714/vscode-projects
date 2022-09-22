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
        AND platform = 'jd'
        AND shop_id = '{{shop_id}}'
        AND session_id GLOBAL NOT IN (
            SELECT DISTINCT
                session_id
            FROM ft_dwd.session_detail_all
            WHERE day = {{ds_nodash}}
            AND platform = 'jd'
            AND shop_id = '{{shop_id}}'
        )
        ORDER BY session_id, msg_time ASC
    )
    GROUP BY day, platform, shop_id, shop_name, session_id, snick, cnick, real_buyer_nick
)
