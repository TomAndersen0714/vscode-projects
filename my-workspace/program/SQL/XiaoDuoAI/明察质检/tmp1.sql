INSERT INTO {sink_table}
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
                m_session_send_cnt,
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
                    groupArray(msg_time) AS _msg_times,
                    groupArray(send_msg_from) AS _send_msg_froms,
                    -- 剔除非人工发送消息
                    arrayMap(
                        (x,y)->(if(x='send_msg' AND y!=2, 0, 1)), _msg_acts, _send_msg_froms
                    ) AS manual_tags, -- 剔除后保留标记
                    arrayFilter(
                        (x,y)->(y=1), _msg_acts, manual_tags
                    ) AS _manual_msg_acts,
                    arrayFilter(
                        (x,y)->(y=1), _msg_times, manual_tags
                    ) AS _manual_msg_times,

                    -- 切分会话生成QA切分标记, PS: 可能存在单个Q, 单个A, 单个QA, 多个QA四种情况, 此切分方法只能切分多QA的情况
                    arrayMap(
                        (x, y)->(if(x = 'recv_msg' AND _msg_acts[y-1] = 'send_msg', 1, 0)),
                        _msg_acts,
                        arrayEnumerate(_msg_acts)
                    ) AS qa_split_tags,
                    -- 切分act和msg_time
                    arraySplit((x, y)->(y), _msg_acts, qa_split_tags) AS _split_msg_acts,
                    arraySplit((x, y)->(y), _msg_times, qa_split_tags) AS _split_msg_times,
                    -- 生成QA标记
                    arrayMap((x)->(has(x, 'recv_msg') AND has(x, 'send_msg')), _split_msg_acts) AS _qa_tags,
                    -- 筛选QA的act和msg_time段
                    arrayFilter((x,y)->(y=1), _split_msg_acts, _qa_tags) AS qa_msg_acts,
                    arrayFilter((x,y)->(y=1), _split_msg_times, _qa_tags) AS qa_msg_times,
                    -- 遍历QA的act和msg_time段, 计算首次recv和send的时间差, PS: 如果当前会话不存在QA, 则结果为空数组
                    arrayMap(
                        (x,y)->(parseDateTimeBestEffort(toString(y[indexOf(x, 'send_msg')])) - parseDateTimeBestEffort(toString(y[indexOf(x, 'recv_msg')]))),
                        qa_msg_acts,
                        qa_msg_times
                    ) AS qa_reply_intervals_secs,

                    -- 切分剔除了非人工发送消息的会话, 生成QA切分标记, PS: 可能存在单个Q, 单个A, 单个QA, 多个QA四种情况, 此切分方法只能切分多QA的情况
                    arrayMap(
                        (x, y)->(if(x = 'recv_msg' AND _manual_msg_acts[y-1] = 'send_msg', 1, 0)),
                        _manual_msg_acts,
                        arrayEnumerate(_manual_msg_acts)
                    ) AS manual_qa_split_tags,
                    -- 切分act和msg_time
                    arraySplit((x, y)->(y), _manual_msg_acts, manual_qa_split_tags) AS _manual_split_msg_acts,
                    arraySplit((x, y)->(y), _manual_msg_times, manual_qa_split_tags) AS _manual_split_msg_times,
                    -- 生成QA标记
                    arrayMap((x)->(has(x, 'recv_msg') AND has(x, 'send_msg')), _manual_split_msg_acts) AS _manual_qa_tags,
                    -- 筛选QA的act和msg_time段
                    arrayFilter((x,y)->(y=1), _manual_split_msg_acts, _manual_qa_tags) AS manual_qa_msg_acts,
                    arrayFilter((x,y)->(y=1), _manual_split_msg_times, _manual_qa_tags) AS manual_qa_msg_times,
                    -- 遍历QA的act和msg_time段, 计算首次recv和send的时间差, PS: 如果当前会话不存在QA, 则结果为空数组
                    arrayMap(
                        (x,y)->(parseDateTimeBestEffort(toString(y[indexOf(x, 'send_msg')])) - parseDateTimeBestEffort(toString(y[indexOf(x, 'recv_msg')]))),
                        manual_qa_msg_acts,
                        manual_qa_msg_times
                    ) AS manual_qa_reply_intervals_secs,

                    -- QA数量
                    length(qa_msg_acts) AS qa_cnt,
                    length(manual_qa_msg_acts) AS manual_qa_cnt,
                    -- 首次响应时长
                    if(empty(qa_reply_intervals_secs), -1, qa_reply_intervals_secs[1]) AS first_reply_interval_secs,
                    if(empty(manual_qa_reply_intervals_secs), -1, manual_qa_reply_intervals_secs[1]) AS manual_first_reply_interval_secs,
                    -- 平均响应时长
                    if(empty(qa_reply_intervals_secs), -1, arrayAvg(qa_reply_intervals_secs)) AS avg_first_reply_interval_secs,
                    if(empty(manual_qa_reply_intervals_secs), -1, arrayAvg(manual_qa_reply_intervals_secs)) AS avg_manual_first_reply_interval_secs,

                    arrayMap(
                        (x,y,z)->(
                            x !='' AND z='recv_msg' AND not has(
                                arrayFilter(
                                    (x,y) -> (y='send_msg'),
                                    arraySlice(_focus_goods_ids, 1, y-1),
                                    arraySlice(_msg_acts, 1, y-1)
                                ),
                                x
                            )
                        ), 
                        _focus_goods_ids,
                        arrayEnumerate(_focus_goods_ids),
                        _msg_acts
                    ) AS _is_c_active_focus_goods_ids,
                    arrayMap(
                        (x,y,z)->(
                            x !='' AND z='send_msg' AND not has(
                                arrayFilter(
                                    (x,y) -> (y='recv_msg'),
                                    arraySlice(_focus_goods_ids, 1, y-1),
                                    arraySlice(_msg_acts, 1, y-1)
                                ),
                                x
                            )
                        ), 
                        _focus_goods_ids,
                        arrayEnumerate(_focus_goods_ids),
                        _msg_acts
                    ) AS _is_s_active_focus_goods_ids,
                    arrayFilter((x,y,z)->(y='recv_msg' AND z=1), _focus_goods_ids, _msg_acts, _is_c_active_focus_goods_ids) AS c_active_send_goods_ids,
                    arrayFilter((x,y,z)->(y='send_msg' AND z=1), _focus_goods_ids, _msg_acts, _is_s_active_focus_goods_ids) AS s_active_send_goods_ids,
                    toString(min(msg_time)) AS session_start_time,
                    toString(max(msg_time)) AS session_end_time,
                    toString(minIf(msg_time, act='recv_msg')) AS recv_msg_start_time,
                    toString(maxIf(msg_time, act='recv_msg')) AS recv_msg_end_time,
                    toString(minIf(msg_time, act='send_msg')) AS send_msg_start_time,
                    toString(maxIf(msg_time, act='send_msg')) AS send_msg_end_time,
                    SUM(act = 'recv_msg') AS session_recv_cnt,
                    SUM(act = 'send_msg') AS session_send_cnt,
                    SUM(act = 'send_msg' AND send_msg_from = 2) AS m_session_send_cnt
                FROM (
                    SELECT
                        day, platform, shop_id, shop_name,
                        session_id, snick, cnick, real_buyer_nick,
                        act, plat_goods_id, msg_time, send_msg_from
                    FROM ft_dwd.session_msg_detail_all
                    WHERE day = {ds_nodash}
                    AND platform = '{platform}'
                    AND shop_id = '{shop_id}'
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
                WHERE day = {ds_nodash}
                AND platform = '{platform}'
                AND shop_id = '{shop_id}'
            ) AS transfer_msg_info
            ON session_info.day = transfer_msg_info.day
            AND session_info.shop_id = transfer_msg_info.shop_id
            AND session_info.snick = transfer_msg_info.from_snick
            -- jd使用real_buyer_nick关联
            AND session_info.cnick = transfer_msg_info.real_buyer_nick
            WHERE toDateTime64(create_time, 0) >= toDateTime64(session_end_time, 0)
            AND toDateTime64(create_time, 0) <= toDateTime64(session_end_time, 0) + 600
            ORDER BY session_id, transfer_time DESC
            LIMIT 1 BY session_id