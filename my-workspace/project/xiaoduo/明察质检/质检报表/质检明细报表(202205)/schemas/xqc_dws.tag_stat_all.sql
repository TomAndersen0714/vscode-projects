-- 表1:
--  统计维度: 天/平台/店铺/子账号/质检项-质检项分组
--  关联数据: 质检项标签名, 质检项分组名
--  统计数据: 触发次数和分值
--  PS: 包含质检词的次数统计

tag_type:
'ai_abnormal',
'ai_excellent',
'ai_s_emotion',
'ai_c_emotion',
'manual_subtract',
'manual_add',
'custom_subtract',
'custom_add',
'custom_message',
'custom_dialog'

-- xqc_dws.tag_stat_local
CREATE TABLE xqc_dws.tag_stat_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `seller_nick` String,
    `snick` String,
    `tag_group_id` String,
    `tag_group_name` String,
    `tag_type` String,
    `tag_id` String,
    `tag_name` String,
    `tag_cnt_sum` Int64,
    `tag_score_sum` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY `day`
ORDER BY (platform, seller_nick, snick)
SETTINGS storage_policy = 'rr', index_granularity = 8192


-- xqc_dws.tag_stat_all
-- DROP TABLE xqc_dws.tag_stat_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dws.tag_stat_all ON CLUSTER cluster_3s_2r
AS xqc_dws.tag_stat_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dws', 'tag_stat_local', rand())


-- 旧版本AI质检项
INSERT INTO xqc_dws.tag_stat_all
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
        tag_type,
        tag_id,
        SUM(tag_cnt) AS tag_cnt_sum,
        -- 同一个ID分数可能发生变化, 以实际打标为准
        SUM(tag_score*tag_cnt) AS tag_score_sum
    FROM (
        -- 旧版本AI质检项-非情绪扣分项
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'ai_abnormal' AS tag_type,
            arrayMap((x)->toString(x), abnormals_type) AS tag_ids,
            abnormals_count AS tag_cnts,
            arrayResize([0], length(abnormals_type)) AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        -- 过滤旧版本AI质检
        AND (abnormals_rule_id = [] OR arrayAll((x)->x='',abnormals_rule_id))

        -- 旧版本AI质检项-非情绪加分项
        UNION ALL
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'ai_excellent' AS tag_type,
            arrayMap((x)->toString(x), excellents_type) AS tag_ids,
            excellents_count AS tag_cnts,
            arrayResize([0], length(excellents_type)) AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        -- 过滤旧版本AI质检
        AND (excellents_rule_id = [] OR arrayAll((x)->x='',excellents_rule_id))

        -- 旧版本AI质检项-买家情绪项
        UNION ALL
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'ai_c_emotion' AS tag_type,
            arrayMap((x)->toString(x), c_emotion_type) AS tag_ids,
            c_emotion_count AS tag_cnts,
            arrayResize([0], length(c_emotion_type)) AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        -- 过滤旧版本AI质检
        AND (c_emotion_rule_id = [] OR arrayAll((x)->x='',c_emotion_rule_id))

        -- 旧版本AI质检项-客服情绪项
        UNION ALL
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'ai_s_emotion' AS tag_type,
            arrayMap((x)->toString(x), s_emotion_type) AS tag_ids,
            s_emotion_count AS tag_cnts,
            arrayResize([0], length(s_emotion_type)) AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        -- 过滤旧版本AI质检
        AND (s_emotion_rule_id = [] OR arrayAll((x)->x='',s_emotion_rule_id))
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
    -- 关联维度信息
    SELECT
        qc_rule_type AS tag_type,
        qc_rule_id AS tag_id,
        qc_rule_name AS tag_name,
        qc_rule_group_id AS tag_group_id,
        qc_rule_group_name AS tag_group_name
    FROM xqc_dim.qc_rule_constant_all
    WHERE day = {snapshot_ds_nodash}
) AS dim_tag
USING(tag_type, tag_id)


-- 新版本AI质检项
INSERT INTO xqc_dws.tag_stat_all
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
            'ai_abnormal' AS tag_type,
            abnormals_rule_id AS tag_ids,
            abnormals_count AS tag_cnts,
            abnormals_score AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        -- 过滤新版本AI质检
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
        -- 过滤新版本AI质检
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
        -- 过滤新版本AI质检
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
        -- 过滤新版本AI质检
        AND arrayExists((x)->x!='',s_emotion_rule_id)
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


-- 人工质检项
INSERT INTO xqc_dws.tag_stat_all
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
        tag_type,
        tag_id,
        SUM(tag_cnt + if(tag_md>0, 1, 0)) AS tag_cnt_sum,
        -- 同一个ID分数可能发生变化, 以实际打标为准
        SUM(tag_score*(tag_cnt + if(tag_md>0, 1, 0))) AS tag_score_sum
    FROM (
        -- 人工质检项-扣分项
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'manual_subtract' AS tag_type,
            tag_score_stats_id AS tag_ids,
            tag_score_stats_count AS tag_cnts,
            tag_score_stats_score AS tag_scores,
            -- 是否打标在会话上
            if(
                tag_score_stats_md=[],
                arrayResize([0], length(tag_score_stats_id)),
                tag_score_stats_md
            ) AS tag_mds
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        AND tag_score_stats_id != []

        -- 人工质检项-加分项
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
            -- 是否打标在会话上
            if(
                tag_score_add_stats_md=[],
                arrayResize([0], length(tag_score_add_stats_id)),
                tag_score_add_stats_md
            ) AS tag_mds
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        AND tag_score_add_stats_id != []

    ) AS ods_manual_tag
    ARRAY JOIN
        tag_ids AS tag_id,
        tag_mds AS tag_md,
        tag_cnts AS tag_cnt,
        tag_scores AS tag_score
    -- 排除空数据
    WHERE (tag_cnt!=0 OR tag_md>0)
    GROUP BY
        day,
        platform,
        seller_nick,
        snick,
        tag_type,
        tag_id
) AS ods_manual_tag_stat
GLOBAL LEFT JOIN (
    -- 关联维度信息
    SELECT
        *
    FROM (
        -- 查询人工质检项
        SELECT
            _id AS tag_id,
            name AS tag_name,
            qc_norm_group_id AS tag_group_id
        FROM xqc_dim.qc_rule_all
        WHERE day = {snapshot_ds_nodash}
        AND rule_category = 2
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


-- 自定义质检项
INSERT INTO xqc_dws.tag_stat_all
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
        tag_type,
        tag_id,
        SUM(tag_cnt) AS tag_cnt_sum,
        -- 同一个ID分数可能发生变化, 以实际打标为准
        SUM(tag_score*tag_cnt) AS tag_score_sum
    FROM (
        -- 旧版本自定义质检项-扣分项
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'custom_subtract' AS tag_type,
            rule_stats_id AS tag_ids,
            rule_stats_count AS tag_cnts,
            rule_stats_score AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        AND rule_stats_id != []

        -- 旧版本自定义质检项-加分项
        UNION ALL
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'custom_add' AS tag_type,
            rule_add_stats_id AS tag_ids,
            rule_add_stats_count AS tag_cnts,
            rule_add_stats_score AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        AND rule_add_stats_id != []

        -- 新版本自定义质检项-消息质检项
        UNION ALL
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'custom_message' AS tag_type,
            xrule_stats_id AS tag_ids,
            xrule_stats_count AS tag_cnts,
            xrule_stats_score AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        AND xrule_stats_id != []

        -- 新版本自定义质检项-会话质检项
        UNION ALL
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            'custom_dialog' AS tag_type,
            top_xrules_id AS tag_ids,
            top_xrules_count AS tag_cnts,
            top_xrules_score AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
        AND top_xrules_id != []

    ) AS ods_custom_tag
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
) AS ods_custom_tag_stat
GLOBAL LEFT JOIN (
    -- 关联维度信息
    SELECT
        *
    FROM (
        -- 查询自定义质检项
        SELECT
            _id AS tag_id,
            name AS tag_name,
            qc_norm_group_id AS tag_group_id
        FROM xqc_dim.qc_rule_all
        WHERE day = {snapshot_ds_nodash}
        AND rule_category = 3
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
