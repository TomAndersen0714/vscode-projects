-- 指标:



-- 维度关联: 
-- 质检标准-子账号分组-子账号
-- 质检标准-质检项分组-质检项

-- 表1:
--      统计维度: 天/平台/店铺/子账号/质检项分组/质检项
--      关联数据: 质检项标签名, 质检项分组名, 子账号对应员工最新信息
--      统计数据: 触发次数和分值
--      PS: 包含质检词的次数统计
dws.xqc_tag_stat_all

-- 旧版本AI质检项
INSERT INTO dws.xqc_tag_stat_all
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
    SUM(tag_cnt_sum) AS tag_cnt_sum,
    SUM(tag_score_sum) AS tag_score_sum
FROM (
    SELECT
        day,
        platform,
        seller_nick,
        snick,
        tag_type,
        tag_id,
        SUM(tag_cnt) AS tag_cnt_sum,
        SUM(tag_score) AS tag_score_sum
    FROM (
        -- 旧版本AI质检项-非情绪扣分项
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'ai_abnormal' AS tag_type,
            abnormals_type AS tag_ids,
            abnormals_count AS tag_cnts,
            arrayResize([0], length(abnormals_type)) AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        -- 过滤旧版本AI质检
        AND abnormals_rule_id = []

        -- 旧版本AI质检项-非情绪加分项
        UNION ALL
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'ai_excellent' AS tag_type,
            excellents_type AS tag_ids,
            excellents_count AS tag_cnts,
            arrayResize([0], length(excellents_type)) AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        -- 过滤旧版本AI质检
        AND excellents_rule_id = []

        -- 旧版本AI质检项-买家情绪项
        UNION ALL
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'ai_c_emotion' AS tag_type,
            c_emotion_type AS tag_ids,
            c_emotion_count AS tag_cnts,
            arrayResize([0], length(c_emotion_type)) AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        -- 过滤旧版本AI质检
        AND c_emotion_rule_id = []

        -- 旧版本AI质检项-客服情绪项
        UNION ALL
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'ai_s_emotion' AS tag_type,
            s_emotion_type AS tag_ids,
            s_emotion_count AS tag_cnts,
            arrayResize([0], length(s_emotion_type)) AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        -- 过滤旧版本AI质检
        AND s_emotion_rule_id = []
    )
    ARRAY JOIN
        tag_ids AS tag_id,
        tag_cnts AS tag_cnt,
        tag_scores AS tag_score
    -- 排除空数据
    WHERE tag_cnt!=0
    GROUP BY
        day,
        platform,
        seller_nick,
        snick,
        tag_type,
        tag_id
) AS ods_ai_tag
GLOBAL LEFT JOIN (
    SELECT
        qc_rule_type AS tag_type,
        qc_rule_id AS tag_id,
        qc_rule_name AS tag_name,
        qc_rule_group_id AS tag_group_id,
        qc_rule_group_name AS tag_group_name
    FROM xqc_dim.qc_rule_constant_all
    WHERE day = toYYYYMMDD(yesterday())
) AS dim_tag
USING(tag_type, tag_id)
GROUP BY
    day,
    platform,
    seller_nick,
    snick,
    tag_group_id,
    tag_group_name,
    tag_type,
    tag_id,
    tag_name

-- 新版本AI质检项
INSERT INTO dws.xqc_tag_stat_all
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
    SUM(tag_cnt_sum) AS tag_cnt_sum,
    SUM(tag_score_sum) AS tag_score_sum
FROM (
    SELECT
        day,
        platform,
        seller_nick,
        snick,
        tag_type,
        tag_id,
        SUM(tag_cnt) AS tag_cnt_sum,
        SUM(tag_score) AS tag_score_sum
    FROM (
        -- 新版本AI质检项-非情绪扣分项
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'ai_abnormal' AS tag_type,
            abnormals_rule_id AS tag_ids,
            abnormals_count AS tag_cnts,
            abnormals_score AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        -- 过滤新版本AI质检
        AND abnormals_rule_id != []

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
        -- 过滤新版本AI质检
        AND excellents_rule_id != []

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
        -- 过滤新版本AI质检
        AND c_emotion_rule_id = []

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
        -- 过滤新版本AI质检
        AND s_emotion_rule_id != []
    ) AS ods_ai_tag
    ARRAY JOIN
        tag_ids AS tag_id,
        tag_cnts AS tag_cnt,
        tag_scores AS tag_score
    -- 排除空数据
    WHERE tag_cnt!=0
    GROUP BY
        day,
        platform,
        seller_nick,
        snick,
        tag_type,
        tag_id
) AS ods_ai_tag
GLOBAL LEFT JOIN (
    SELECT
        *
    FROM (
        SELECT
            _id AS tag_id,
            name AS tag_name,
            qc_norm_group_id AS tag_group_id
        FROM xqc_dim.qc_rule_all
        WHERE day = toYYYYMMDD(yesterday())
    ) AS dim_tag
    -- PS: 已删除的质检项无法获取到分组信息
    GLOBAL LEFT JOIN (
        SELECT
            _id AS tag_group_id,
            full_name AS tag_group_name
        FROM xqc_dim.qc_norm_group_path_all
        WHERE day = toYYYYMMDD(yesterday())
    ) AS dim_tag_group
    USING(tag_group_id)
) AS dim_tag
USING(tag_id)
GROUP BY
    day,
    platform,
    seller_nick,
    snick,
    tag_group_id,
    tag_group_name,
    tag_type,
    tag_id,
    tag_name

-- 人工质检项
INSERT INTO dws.xqc_tag_stat_all
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
    SUM(tag_cnt_sum) AS tag_cnt_sum,
    SUM(tag_score_sum) AS tag_score_sum
FROM (
    SELECT
        day,
        platform,
        seller_nick,
        snick,
        tag_type,
        tag_id,
        SUM(tag_cnt) AS tag_cnt_sum,
        SUM(tag_score) AS tag_score_sum
    FROM (
        -- 人工质检项-加分项
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'manual_subtract' AS tag_type,
            tag_score_stats_id AS tag_ids,
            tag_score_stats_count AS tag_cnts,
            tag_score_stats_score AS tag_scores,
            if(
                tag_score_stats_md=[],
                arrayResize([0], length(tag_score_stats_id)),
                tag_score_stats_md
            ) AS tag_mds
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}

        -- 人工质检项-扣分项
        UNION ALL
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'manual_add' AS tag_type,
            tag_score_add_stats_id AS tag_ids,
            tag_score_add_stats_count AS tag_cnts,
            tag_score_add_stats_score AS tag_scores,
            if(
                tag_score_add_stats_md=[],
                arrayResize([0], length(tag_score_add_stats_id)),
                tag_score_add_stats_md
            ) AS tag_mds
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}

    ) AS ods_ai_tag
    ARRAY JOIN
        tag_ids AS tag_id,
        tag_mds AS tag_md,
        tag_cnts AS tag_cnt,
        tag_scores AS tag_score
    -- 排除空数据
    WHERE tag_cnt!=0
    GROUP BY
        day,
        platform,
        seller_nick,
        snick,
        tag_type,
        tag_id
) AS ods_ai_tag
GLOBAL LEFT JOIN (
    SELECT
        *
    FROM (
        SELECT
            _id AS tag_id,
            name AS tag_name,
            qc_norm_group_id AS tag_group_id
        FROM xqc_dim.qc_rule_all
        WHERE day = toYYYYMMDD(yesterday())
    ) AS dim_tag
    -- PS: 已删除的质检项无法获取到分组信息
    GLOBAL LEFT JOIN (
        SELECT
            _id AS tag_group_id,
            full_name AS tag_group_name
        FROM xqc_dim.qc_norm_group_path_all
        WHERE day = toYYYYMMDD(yesterday())
    ) AS dim_tag_group
    USING(tag_group_id)
) AS dim_tag
USING(tag_id)
GROUP BY
    day,
    platform,
    seller_nick,
    snick,
    tag_group_id,
    tag_group_name,
    tag_type,
    tag_id,
    tag_name

-- 人工质检项



-- 表2:
--      统计维度: 天/平台/店铺/子账号
--      统计数据: 会话总量/各个质检类别的分值等等




-- 表3:
--     过滤维度: last_mark_id
--     关联数据: 质检员和子账号对应的员工信息
--     统计数据无:
--     PS: 仅过滤出发生质检的会话数据, 表结构同dwd.xdqc_dialog_all
--     PS: 此数据后续用于统计质检员检查量, 以及客服的被检查量Top10