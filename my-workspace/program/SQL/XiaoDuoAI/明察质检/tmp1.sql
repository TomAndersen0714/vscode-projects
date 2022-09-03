SELECT
    message_tag_info.*,
    dialog_tag_info.dialog_tag_names
FROM (
    SELECT
        day,
        dialog_id,
        message_id,
        snick,
        cnick,
        act,
        content,
        time,
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
            time,
            qid,
            answer_explain,
            send_from,
            algo_emotion,
            abnormal_model,
            excellent_model,
            tag_type,
            toString(tag_num) AS tag_id
        FROM (
            SELECT
                day,
                dialog_id,
                _id AS message_id,
                snick,
                cnick,
                if(source=1, 'send_msg', 'recv_msg') AS act,
                content,
                toString(toDateTime64(time, 0, 'UTC') + 8*3600) AS time,
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
                    arrayResize(['not_ai'], length(xrule_stats.id), 'not_ai'),

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
                    xrule_stats.id,

                    -- 人工质检
                    tags.tag_id
                ) AS tag_nums
            FROM xqc_ods.message_replay_all
            WHERE day = 20220901
            -- 筛选重返数据
            AND cnick LIKE '%重放%'
            -- 筛选指定批次
            AND xxHash64(seller_nick) % 16 = 1
        ) AS msg_ai_tags
        ARRAY JOIN
            tag_types AS tag_type,
            tag_nums AS tag_num
    ) AS msg_ai_tag
    GLOBAL LEFT JOIN (
        -- AI质检项标签信息
        SELECT DISTINCT
            qc_rule_type AS tag_type,
            qc_rule_id AS tag_id,
            qc_rule_name AS tag_name
        FROM xqc_dim.qc_rule_constant_all
        WHERE day = toYYYYMMDD(yesterday())
        
        UNION ALL
        SELECT DISTINCT
            'ai_s_emotion' AS tag_type,
            '0' AS tag_id,
            '中性' AS tag_name
        
        -- 非AI质检项标签信息-人工质检标签+自定义质检标签
        UNION ALL
        SELECT DISTINCT
            'not_ai' AS tag_type,
            _id AS tag_id,
            name AS tag_name
        FROM xqc_dim.qc_rule_all
        WHERE day = toYYYYMMDD(yesterday())
        -- 筛选非AI质检项
        AND rule_category != 1
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
        time,
        qid,
        answer_explain,
        send_from,
        algo_emotion,
        abnormal_model,
        excellent_model
) AS message_tag_info
GLOBAL LEFT JOIN (
    SELECT
        dialog_id,
        groupArray(tag_name) AS dialog_tag_names
    FROM (
        SELECT *,tag_id FROM (
            SELECT
                _id AS dialog_id,
                -- 会话级别质检项
                arrayConcat(
                    -- 自定义质检项-会话级别
                    top_xrules_id,
                    -- 人工质检项-加分-会话级别
                    arrayFilter((x,y)->y>0, tag_score_stats_id, tag_score_stats_md),
                    -- 人工质检项-扣分-会话级别
                    arrayFilter((x,y)->y>0, tag_score_add_stats_id, tag_score_add_stats_md)
                ) AS tag_ids
            FROM xqc_ods.dialog_replay_all
            WHERE day = 20220901
            -- 筛选重返数据
            AND cnick LIKE '%重放%'
            -- 筛选指定批次
            AND xxHash64(seller_nick) % 16 = 1
        )
        ARRAY JOIN
            tag_ids AS tag_id
    ) AS dialog_info
    GLOBAL LEFT JOIN (
        SELECT
            _id AS tag_id,
            name AS tag_name
        FROM xqc_dim.qc_rule_all
        WHERE day = toYYYYMMDD(yesterday())
    ) AS tag_info
    USING(tag_id)
    GROUP BY dialog_id
) AS dialog_tag_info
USING(dialog_id)
ORDER BY day, dialog_id, time