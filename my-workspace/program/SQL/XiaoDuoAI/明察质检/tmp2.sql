    
SELECT
    day,
    platform,
    seller_nick,
    snick,
    dim_tag.tag_group_id AS tag_group_id,
    dim_tag.tag_group_name AS tag_group_name,
    tag_type,
    tag_id,
    dim_tag.tag_name AS tag_name,
    tag_cnt_sum,
    tag_score_sum
FROM (
    SELECT
        day,
        platform,
        seller_nick,
        snick,
        cnick,
        _id AS dialog_id,
        tag_type,
        tag_id,
        SUM(tag_cnt) AS tag_cnt_sum,
        -- 同一个ID分数可能发生变化, 以实际打标为准
        SUM(tag_score*tag_cnt) AS tag_score_sum
    FROM (
        -- 新版本AI质检项-非情绪扣分项
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            cnick,
            _id AS dialog_id,
            order_info_id[1] AS order_id,
            focus_goods_id,
            abnormals_rule_id AS tag_ids,
            abnormals_count AS tag_cnts,
            abnormals_score AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        -- 过滤新版本AI质检-扣分项
        AND arrayExists((x)->x!='',abnormals_rule_id)

        -- 新版本AI质检项-非情绪加分项
        UNION ALL
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'ai_excellent' AS tag_type,
            excellents_rule_id AS tag_ids,
            excellents_count AS tag_cnts,
            excellents_score AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        -- 过滤新版本AI质检-加分项
        AND arrayExists((x)->x!='',excellents_rule_id)

        -- 新版本AI质检项-买家情绪项
        UNION ALL
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'ai_c_emotion' AS tag_type,
            c_emotion_rule_id AS tag_ids,
            c_emotion_count AS tag_cnts,
            c_emotion_score AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        -- 过滤新版本AI质检-买家情绪项
        AND arrayExists((x)->x!='',c_emotion_rule_id)

        -- 新版本AI质检项-客服情绪项
        UNION ALL
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'ai_s_emotion' AS tag_type,
            s_emotion_rule_id AS tag_ids,
            s_emotion_count AS tag_cnts,
            s_emotion_score AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        -- 过滤新版本AI质检-客服情绪项
        AND arrayExists((x)->x!='',s_emotion_rule_id)
    ) AS ods_ai_tag
    ARRAY JOIN
        tag_ids AS tag_id,
        tag_cnts AS tag_cnt,
        tag_scores AS tag_score
    -- 排除空数据
    WHERE tag_id!='' AND tag_cnt!=0
    GROUP BY
        day,
        platform,
        seller_nick,
        snick,
        tag_type,
        tag_id
) AS ods_ai_tag
GLOBAL LEFT JOIN (
    -- 关联维度信息
    SELECT
        *
    FROM (
        -- 查询AI质检项
        SELECT
            _id AS tag_id,
            name AS tag_name,
            qc_norm_group_id AS tag_group_id
        FROM xqc_dim.qc_rule_all
        WHERE day = {snapshot_ds_nodash}
        AND rule_category = 1
    ) AS dim_tag
    GLOBAL LEFT JOIN (
        -- 关联质检项分组
        -- PS: 已删除的分组无法获取
        SELECT
            _id AS tag_group_id,
            full_name AS tag_group_name
        FROM xqc_dim.qc_norm_group_full_all
        WHERE day = {snapshot_ds_nodash}
    ) AS dim_tag_group
    USING(tag_group_id)
) AS dim_tag
USING(tag_id)
    