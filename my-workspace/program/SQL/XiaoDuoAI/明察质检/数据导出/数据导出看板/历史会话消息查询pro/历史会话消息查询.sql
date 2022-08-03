-- 数据导出-明察质检-历史会话消息查询
-- 查询条件： AI质检
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
                        -- 自定义质检-会话质检项
                        top_xrules_id,

                        -- 人工质检-扣分标签ID
                        tag_score_stats_id,
                        -- 人工质检-加分标签ID
                        tag_score_add_stats_id
                    ) AS tag_ids
                FROM dwd.xdqc_dialog_all
                WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=31-day-ago }}'))
                    AND toYYYYMMDD(toDate('{{ day.end=31-day-ago }}'))
                -- 下拉框-平台
                AND platform = '{{ platform=tb }}'
                -- 下拉框-店铺主账号
                AND seller_nick = '{{ seller_nick=方太官方旗舰店 }}'
                -- 过滤空数据
                AND tag_ids!=[]
                -- 下拉框-质检项
                AND (
                    '{{ tag_ids }}'=''
                    OR
                    hasAny(tag_ids, splitByChar(',','{{ tag_ids }}'))
                )
                -- 下拉框-限制会话数量
                LIMIT {{ dialog_limit_num=1 }}
            ) AS dialog_tag_info
        ) AS dialog_id_list
        -- AI质检-非情绪扣分项
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

            -- 质检项ID
            arrayConcat(
                -- AI质检项
                abnormal,
                excellent,

                --
            ) AS tag_ids

        FROM xqc_ods.message_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=31-day-ago }}')) 
            AND toYYYYMMDD(toDate('{{ day.end=31-day-ago }}'))
        AND platform = 'tb'
        -- 下拉框-店铺名
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