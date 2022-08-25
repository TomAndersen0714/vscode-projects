-- 历史会话消息查询-数据导出-明察质检-pro
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
    groupArray(tag_name) AS tag_names,
    '{{ qc_norm_ids=624e7765befbc1ec1606aa81 }}' AS qc_norm_ids,
    '{{ qc_norm_group_ids=624e7765befbc1ec1606aa8e }}' AS qc_norm_group_ids
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
        toString(tag_num) AS tag_id
    FROM (
        WITH (
            -- 筛选满足条件的dialog_id
            SELECT groupUniqArray(dialog_id) AS dialog_id_list
            FROM (
                SELECT
                    _id AS dialog_id,
                    -- 质检项ID
                    arrayConcat(
                        -- AI质检-非情绪扣分项
                        arrayFilter(
                            (x, y) -> y>0,
                            abnormals_rule_id,
                            abnormals_count
                        ),
                        -- AI质检-非情绪加分项
                        arrayFilter(
                            (x, y) -> y>0,
                            excellents_rule_id,
                            excellents_count
                        ),
                        -- AI质检-买家情绪项
                        arrayFilter(
                            (x, y) -> y>0,
                            c_emotion_rule_id,
                            c_emotion_count
                        ),
                        -- AI质检-客服情绪项
                        arrayFilter(
                            (x, y) -> y>0,
                            s_emotion_rule_id,
                            s_emotion_count
                        ),

                        -- 自定义质检-消息质检项
                        xrule_stats_id,
                        -- -- 自定义质检-会话质检项
                        -- top_xrules_id,

                        -- 人工质检-扣分标签ID
                        tag_score_stats_id,
                        -- 人工质检-加分标签ID
                        tag_score_add_stats_id
                    ) AS tag_ids
                FROM dwd.xdqc_dialog_all
                WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=31-day-ago }}'))
                    AND toYYYYMMDD(toDate('{{ day.end=31-day-ago }}'))
                -- 下拉框-平台
                AND platform = 'tb'
                -- 下拉框-店铺主账号
                AND seller_nick = '{{ seller_nick=方太官方旗舰店 }}'
                -- 过滤空数据
                AND tag_ids!=[]
                -- 下拉框-质检项
                AND hasAny(tag_ids, splitByChar(',','{{ tag_ids=624e7765befbc1ec1606aa96 }}'))
                -- 下拉框-限制会话数量
                LIMIT {{ dialog_limit_num=1 }}
            ) AS dialog_tag_info
        ) AS dialog_id_list
        -- AI质检-旧版本AI质检项
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
            arrayConcat(
                -- AI质检项类型
                arrayResize(['ai_abnormal'], length(abnormal), 'ai_abnormal'),
                arrayResize(['ai_excellent'], length(excellent), 'ai_excellent'),
                [if(source=1, 'ai_s_emotion', 'ai_c_emotion')],

                -- 自定义质检项
                arrayResize(['not_ai'], length(rule_stats.id), 'not_ai'),
                arrayResize(['not_ai'], length(rule_add_stats.id), 'not_ai'),

                -- 人工质检项
                arrayResize(['not_ai'], length(tags.tag_id), 'not_ai')

            ) AS tag_types,

            -- 质检项ID
            arrayConcat(
                -- AI质检-
                arrayMap(x->toString(x), abnormal),
                arrayMap(x->toString(x), excellent),
                [toString(emotion)],

                -- 自定义质检
                rule_stats.id,
                rule_add_stats.id,

                -- 人工质检
                tags.tag_id
            ) AS tag_nums
        FROM xqc_ods.message_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=31-day-ago }}')) 
            AND toYYYYMMDD(toDate('{{ day.end=31-day-ago }}'))
        AND platform = 'tb'
        -- 下拉框-店铺名
        AND seller_nick = '{{ seller_nick=方太官方旗舰店 }}'
        -- 过滤指定会话
        AND has(dialog_id_list, dialog_id)

    ) AS msg_ai_tags
    ARRAY JOIN
        tag_types AS tag_type,
        tag_nums AS tag_num
) AS msg_ai_tag
GLOBAL LEFT JOIN (
    -- AI质检项标签信息
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
    
    -- 非AI质检项标签信息-人工质检标签+自定义质检标签
    UNION ALL
    SELECT
        'not_ai' AS tag_type,
        _id AS tag_id,
        name AS tag_name
    FROM xqc_dim.qc_rule_all
    WHERE day = toYYYYMMDD(yesterday())
    -- 筛选非AI质检项
    AND rule_category != 1
    -- 下拉框-质检项
    AND _id IN splitByChar(',','{{ tag_ids=624e7765befbc1ec1606aa96 }}')

) AS dim_tags
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