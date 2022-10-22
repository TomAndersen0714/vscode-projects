SELECT `day`,
    platform,
    shop_id,
    shop_name,
    snick,
    session_id,
    if(t2.platform = '', -1, t2.first_reply_interval_secs) AS first_reply_interval_secs
FROM (
        SELECT DISTINCT `day`,
            platform,
            shop_id,
            shop_name,
            snick,
            session_id
        FROM ft_dwd.session_msg_detail_all
        WHERE `day` = 20220918
            AND shop_id = '5cac112e98ef4100118a9c9f'
    ) AS t1
    LEFT JOIN (
        SELECT `day`,
            platform,
            shop_id,
            shop_name,
            snick,
            session_id,
            if(
                length(first_reply_interval_secs_arr_filter) = 0,
                -1,
                first_reply_interval_secs_arr_filter [1]
            ) AS first_reply_interval_secs
        FROM (
                SELECT `day`,
                    platform,
                    shop_id,
                    shop_name,
                    snick,
                    session_id,
                    -- 将消息时间按照QA进行切分
                    arraySplit((x, y)->(y), msg_time_arr, qa_tag_arr) AS _msg_time_arr_arr,
                    -- 将消息动作按照QA进行切分
                    arraySplit((x, y)->(y), _msg_acts, qa_tag_arr) AS _msg_act_arr_arr,
                    -- 筛选有QA的动作
                    arrayFilter(
                        x->has(x, 'recv_msg') = 1 AND has(x, 'send_msg') = 1,
                        _msg_act_arr_arr
                    ) AS _msg_act_filtered_arr_arr,
                    -- 筛选有QA的动作对应的消息时间
                    arrayFilter(
                        (x, y)->has(y, 'recv_msg') = 1 AND has(y, 'send_msg') = 1,
                        _msg_time_arr_arr,
                        _msg_act_arr_arr
                    ) AS msg_time_arr_arr_filter,
                    -- 计算各个QA中首次回复时间
                    arrayMap(
                        (x, y)->(
                            if(indexOf(y, 'send_msg') = 0, -1, x [indexOf(y,'send_msg')] - x [1])
                        ),
                        msg_time_arr_arr_filter,
                        _msg_act_filtered_arr_arr
                    ) AS first_reply_interval_secs_arr,
                    arrayFilter(x->x != -1, first_reply_interval_secs_arr) AS first_reply_interval_secs_arr_filter
                FROM (
                        SELECT `day`,
                            platform,
                            shop_id,
                            shop_name,
                            snick,
                            session_id,
                            groupArray(act) AS _msg_acts,
                            groupArray(msg_time) AS msg_time_arr,
                            arrayMap(
                                (x, y)->(if(x = 'recv_msg' AND _msg_acts[y-1] = 'send_msg', 1, 0)),
                                _msg_acts,
                                arrayEnumerate(_msg_acts)
                            ) AS qa_tag_arr
                        FROM (
                                SELECT `day`,
                                    platform,
                                    shop_id,
                                    shop_name,
                                    snick,
                                    session_id,
                                    act,
                                    msg_time
                                FROM ft_dwd.session_msg_detail_all
                                WHERE `day` = 20220918
                                    AND shop_id = '5cac112e98ef4100118a9c9f'
                                ORDER BY `day`,
                                    platform,
                                    shop_id,
                                    shop_name,
                                    snick,
                                    session_id,
                                    msg_time
                            )
                        GROUP BY `day`,
                            platform,
                            shop_id,
                            shop_name,
                            snick,
                            session_id
                ) AS qa_split
                -- WHERE has(qa_tag_arr, 1) = 1
            )
        WHERE length(first_reply_interval_secs_arr_filter) != 0
        ORDER BY snick
    ) AS t2 USING(session_id)