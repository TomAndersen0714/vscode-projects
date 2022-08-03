-- 数据导出-明察质检-历史会话消息查询
-- 查询条件： AI质检项
SELECT
    day,
    dialog_id,
    message_id,
    snick,
    cnick,
    act,
    content,
    create_time,
    qid,
    answer_explain,
    send_from,
    algo_emotion,
    abnormal_model,
    excellent_model,
    groupArray(tag_name) AS tag_names
FROM (
    SELECT
        day,
        dialog_id,
        message_id,
        snick,
        cnick,
        act,
        content,
        create_time,
        qid,
        answer_explain,
        send_from,
        algo_emotion,
        abnormal_model,
        excellent_model,
        tag_type,
        toString(tag_id_num) AS tag_id
    FROM (
        WITH (
            -- 筛选满足条件的dialog_id
            SELECT groupArray(dialog_id) AS dialog_id_list
            FROM (
                SELECT DISTINCT
                    dialog_id
                FROM (
                    SELECT
                        _id AS dialog_id,
                        -- 旧版本AI质检项-非情绪扣分项
                        arrayFilter(
                            (x, y) -> y>0,
                            abnormals_type,
                            abnormals_count
                        ) AS abnormals_types,
                        -- 旧版本AI质检项-非情绪加分项
                        arrayFilter(
                            (x, y) -> y>0,
                            excellents_type,
                            excellents_count
                        ) AS excellents_types,

                        -- 旧版本AI质检项-买家情绪项
                        arrayFilter(
                            (x, y) -> y>0,
                            c_emotion_type,
                            c_emotion_count
                        ) AS c_emotion_types,
    
                        -- 旧版本AI质检项-客服情绪项
                        arrayFilter(
                            (x, y) -> y>0,
                            s_emotion_type,
                            s_emotion_count
                        ) AS s_emotion_types

                    FROM dwd.xdqc_dialog_all
                    WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=31-day-ago }}'))
                        AND toYYYYMMDD(toDate('{{ day.end=31-day-ago }}'))
                    AND platform = 'tb'
                    -- 文本框-店铺名
                    AND seller_nick = '{{ seller_nick=方太官方旗舰店 }}'
                    -- 过滤空数据
                    AND (
                        abnormals_types!=[] OR excellents_types!=[] OR c_emotion_types!=[] OR s_emotion_types!=[]
                    )
                    -- 下拉框-旧版本AI质检项-非情绪扣分项
                    AND (
                        '{{ abnormals_types }}'=''
                        OR
                        hasAny([{{ abnormals_types }}], abnormals_types)
                    )
                    -- 下拉框-旧版本AI质检项-加分项
                    AND (
                        '{{ excellents_types }}'=''
                        OR
                        hasAny([{{ excellents_types }}], excellents_types)
                    )
                    -- 下拉框-旧版本AI质检项-买家情绪项
                    AND (
                        '{{ c_emotion_types }}'=''
                        OR
                        hasAny([{{ c_emotion_types }}], c_emotion_types)
                    )
                    -- 下拉框-旧版本AI质检项-客服情绪项
                    AND (
                        '{{ s_emotion_types }}'=''
                        OR
                        hasAny([{{ s_emotion_types }}], s_emotion_types)
                    )
                ) AS ai_tags
                -- 文本框-限制会话数量
                LIMIT toUInt32({{ dialog_limit_num=1 }})
            ) AS dialog_tag_info
        ) AS dialog_id_list
        -- 旧版本AI质检项-非情绪扣分项
        SELECT
            day,
            dialog_id,
            _id AS message_id,
            snick,
            cnick,
            if(source=1, 'send_msg', 'recv_msg') AS act,
            content,
            toString(toDateTime64(create_time, 0, 'UTC') + 8*3600) AS create_time,
            qid,
            answer_explain,
            send_from,
            algo_emotion,
            abnormal_model,
            excellent_model,
            -- 质检项类型
            arrayPushBack(
                arrayConcat(
                    arrayResize(['ai_abnormal'], length(abnormal), 'ai_abnormal'),
                    arrayResize(['ai_excellent'], length(excellent), 'ai_excellent')
                ),
                if(source=1, 'ai_s_emotion', 'ai_c_emotion' )
            ) AS tag_types,
            -- 质检项ID
            arrayPushBack(
                arrayConcat(
                    abnormal,
                    excellent
                ),
                emotion
            ) AS tag_ids

        FROM xqc_ods.message_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=31-day-ago }}')) 
            AND toYYYYMMDD(toDate('{{ day.end=31-day-ago }}'))
        AND platform = 'tb'
        -- 文本框-店铺名
        AND seller_nick = '{{ seller_nick=方太官方旗舰店 }}'
        -- 过滤指定会话
        AND has(dialog_id_list, dialog_id)

    ) AS ai_no_emotion_tag
    ARRAY JOIN
        tag_types AS tag_type,
        tag_ids AS tag_id_num
) AS ai_msg_tag
GLOBAL LEFT JOIN (
    -- 关联AI质检项标签信息
    SELECT
        qc_rule_type AS tag_type,
        qc_rule_id AS tag_id,
        qc_rule_name AS tag_name
    FROM xqc_dim.qc_rule_constant_all
    WHERE day = toYYYYMMDD(yesterday())
    UNION ALL
    SELECT
        'ai_s_emotion' AS tag_type,
        '0' AS tag_id,
        '中性' AS tag_name
) AS dim_tag
USING(tag_type, tag_id)
GROUP BY
    day,
    dialog_id,
    message_id,
    snick,
    cnick,
    act,
    content,
    create_time,
    qid,
    answer_explain,
    send_from,
    algo_emotion,
    abnormal_model,
    excellent_model
ORDER BY day, dialog_id, create_time