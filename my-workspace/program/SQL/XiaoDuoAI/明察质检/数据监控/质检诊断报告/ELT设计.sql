-- 统计维度: 天/平台/主账号/子账号/质检项一级分组
-- 度量: 扣分会话数, 加分会话数
-- 质检项分组统计

-- t1
-- xqc_dim.qc_norm_group_full_all 新增字段

-- t2
-- 获取每个质检标准的当前和一二级分组
SELECT
    tag_info.qc_norm_id,
    tag_info.group_id,
    tag_id,
    group_info.level_1_group_id,
    group_info.level_2_group_id,
    group_info.level
FROM (
    -- 获取质检项信息
    SELECT
        _id AS tag_id,
        qc_norm_id,
        qc_norm_group_id AS group_id
    FROM xqc_dim.qc_rule_all
    WHERE day = {snapshot_ds_nodash}
    AND status = 1
) AS tag_info
GLOBAL LEFT JOIN (
    -- 获取每个质检项分组的一二级分组信息
    SELECT
        qc_norm_id,
        IF(level = 1, _id, super_group_ids[1]) AS level_1_group_id,
        IF(level = 2, _id, super_group_ids[2]) AS level_2_group_id,
        _id AS group_id,
        name AS group_name,
        level
    FROM xqc_dim.qc_norm_group_full_all
    WHERE day = {snapshot_ds_nodash}
) AS group_info
ON tag_info.qc_norm_id = group_info.qc_norm_id
AND tag_info.group_id = group_info.group_id

-- t3
-- 会话关联质检项分组信息, 不保留未关联上的会话

SELECT
    dialog_tag_info.day,
    dialog_tag_info.platform,
    dialog_tag_info.seller_nick,
    dialog_tag_info.snick,
    dialog_tag_info.dialog_id,
    tag_group_info.qc_norm_id,
    tag_group_info.group_id AS tag_group_id,
    tag_group_info.level AS tag_group_level,
    dialog_tag_info.tag_id,
    dialog_tag_info.tag_score,
    tag_group_info.level_1_group_id AS tag_level_1_group_id,
    tag_group_info.level_2_group_id AS tag_level_2_group_id
FROM (
    SELECT
        day, platform, seller_nick, snick, dialog_id,
        tag_id, tag_score
    FROM (
        SELECT
            {ds_nodash} AS day,
            platform,
            seller_nick,
            snick,
            _id AS dialog_id,
            -- 质检项id
            arrayConcat(
                -- AI质检项-扣分项
                arrayFilter(x->x!='', abnormals_rule_id),
                -- AI质检项-加分项
                arrayFilter(x->x!='', excellents_rule_id),
                -- AI质检项-买家情绪项
                arrayFilter(x->x!='', c_emotion_rule_id),
                -- AI质检项-客服情绪项
                arrayFilter(x->x!='', s_emotion_rule_id),
                -- 人工质检项-扣分项
                tag_score_stats_id,
                -- 人工质检项-加分项
                tag_score_add_stats_id,
                -- 自定义质检项-消息质检项
                xrule_stats_id,
                -- 自定义质检项-会话质检项
                top_xrules_id
            ) AS tag_ids,
            -- 质检项分数
            arrayConcat(
                -- AI质检项-扣分项
                arrayFilter((x, y)->y!='', abnormals_score, abnormals_rule_id),
                -- AI质检项-加分项
                arrayFilter((x, y)->y!='', excellents_score, excellents_rule_id),
                -- AI质检项-买家情绪项
                arrayFilter((x, y)->y!='', c_emotion_score, c_emotion_rule_id),
                -- AI质检项-客服情绪项
                arrayFilter((x, y)->y!='', s_emotion_score, s_emotion_rule_id),
                -- 人工质检项-扣分项
                tag_score_stats_score,
                -- 人工质检项-加分项
                tag_score_add_stats_score,
                -- 自定义质检项-消息质检项
                xrule_stats_score,
                -- 自定义质检项-会话质检项
                top_xrules_score
            ) AS tag_scores
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
    ) AS dialog_info
    -- PS: ARRAY JOIN 子句会自动过滤空数组
    ARRAY JOIN
        tag_ids AS tag_id,
        tag_scores AS tag_score
) AS dialog_tag_info
GLOBAL INNER JOIN (
    t2
) AS tag_group_info
USING(tag_id)

-- t4
-- xqc_dws.tag_group_stat_all
SELECT
    day,
    platform,
    seller_nick,
    snick,
    qc_norm_id,
    tag_level_1_group_id AS tag_group_id,
    1 AS tag_group_level,
    uniqExactIf(dialog_id, tag_score>0) AS add_score_dialog_cnt,
    uniqExactIf(dialog_id, tag_score<0) AS subtract_score_dialog_cnt
FROM (
    t3
) AS dialog_tag_group_info
GROUP BY 
    day,
    platform,
    seller_nick,
    snick,
    qc_norm_id,
    tag_level_1_group_id

UNION ALL

SELECT
    day,
    platform,
    seller_nick,
    snick,
    qc_norm_id,
    tag_level_2_group_id AS tag_group_id,
    2 AS tag_group_level,
    uniqExactIf(dialog_id, tag_score>0) AS add_score_dialog_cnt,
    uniqExactIf(dialog_id, tag_score<0) AS subtract_score_dialog_cnt
FROM (
    t3
) AS dialog_tag_group_info
GROUP BY 
    day,
    platform,
    seller_nick,
    snick,
    qc_norm_id,
    tag_level_2_group_id

-- 统计维度: 天/平台/主账号/子账号/质检标准
-- 度量: 扣分会话数, 加分会话数, 质检会话数

-- t1
-- 构建子账号粒度统一维度表
-- xqc_dim.snick_full_info_all

-- t2
-- xqc_dws.snick_stat_all 新增字段 subtract_score_dialog_cnt, add_score_dialog_cnt
SELECT
    day, platform, seller_nick, snick,
    dim_snick_department.employee_id,
    dim_snick_department.employee_name,
    dim_snick_department.department_id,
    dim_snick_department.department_name,
    -- 分值统计-总计
    subtract_score_sum,
    add_score_sum,
    -- 分值统计-AI质检
    ai_subtract_score_sum,
    ai_add_score_sum,
    -- 分值统计-自定义质检
    custom_subtract_score_sum,
    custom_add_score_sum,
    -- 分值统计-人工质检
    manual_subtract_score_sum,
    manual_add_score_sum,
    -- 会话量统计-总计
    dialog_cnt,
    -- 会话量统计-总计-扣分会话量
    subtract_score_dialog_cnt,
    -- 会话量统计-总计-加分会话量统计
    add_score_dialog_cnt,
    -- 会话量统计-总计-被人工质检会话量
    manual_marked_dialog_cnt,
    -- 会话量统计-AI质检
    ai_subtract_score_dialog_cnt,
    ai_add_score_dialog_cnt,
    -- 会话量统计-自定义质检
    custom_subtract_score_dialog_cnt,
    custom_add_score_dialog_cnt,
    -- 会话量统计-人工质检
    manual_subtract_score_dialog_cnt, -- 人工质检扣分会话量
    manual_add_score_dialog_cnt -- 人工质检加分会话量
FROM (
    SELECT
        toYYYYMMDD(begin_time) AS day,
        platform,
        seller_nick,
        snick,
        -- 分值统计-总计
        sum(score) AS subtract_score_sum,
        sum(score_add) AS add_score_sum,
        -- 分值统计-人工质检
        sum(mark_score) AS manual_subtract_score_sum,
        sum(mark_score_add) AS manual_add_score_sum,
        -- 分值统计-自定义质检
        sum(
            arraySum(rule_stats_score)
            +
            negate(arraySum(arrayMap((x,y) -> x*if(y<0,y,0), xrule_stats_count, xrule_stats_score)))
            +
            negate(arraySum(arrayMap((x,y) -> x*if(y<0,y,0), top_xrules_count, top_xrules_score)))
        ) AS custom_subtract_score_sum,
        sum(
            arraySum(rule_add_stats_score)
            +
            arraySum(arrayMap((x,y) -> x*if(y>0,y,0), xrule_stats_count, xrule_stats_score))
            +
            arraySum(arrayMap((x,y) -> x*if(y>0,y,0), top_xrules_count, top_xrules_score))
        ) AS custom_add_score_sum,
        -- 分值统计-AI质检
        subtract_score_sum - manual_subtract_score_sum - custom_subtract_score_sum AS ai_subtract_score_sum,
        add_score_sum - manual_add_score_sum - custom_add_score_sum AS ai_add_score_sum,

        -- 会话量统计-总计
        COUNT(1) AS dialog_cnt,
        -- 会话量统计-扣分会话量
        uniqExactIf(dialog_id, score>0) AS subtract_score_dialog_cnt,
        -- 会话量统计-加分会话量
        uniqExactIf(dialog_id, score_add>0) AS add_score_dialog_cnt,

        -- 会话量统计-AI质检
        sum((
            score - mark_score - (
                arraySum(rule_stats_score)
                +
                negate(arraySum(arrayMap((x,y) -> x*if(y<0,y,0), xrule_stats_count, xrule_stats_score)))
                +
                negate(arraySum(arrayMap((x,y) -> x*if(y<0,y,0), top_xrules_count, top_xrules_score)))
            )) > 0
        ) AS ai_subtract_score_dialog_cnt,
        sum((
            score_add - mark_score_add - (
                arraySum(rule_add_stats_score)
                +
                arraySum(arrayMap((x,y) -> x*if(y>0,y,0), xrule_stats_count, xrule_stats_score))
                +
                arraySum(arrayMap((x,y) -> x*if(y>0,y,0), top_xrules_count, top_xrules_score))
            )) > 0
        ) AS ai_add_score_dialog_cnt,
        -- sum(arraySum(abnormals_count)!=0) AS abnormal_dialog_cnt,
        -- sum(arraySum(excellents_count)!=0) AS excellent_dialog_cnt,
        -- sum(arraySum(c_emotion_count)!=0) AS c_emotion_dialog_cnt,
        -- sum(arraySum(s_emotion_count)!=0) AS s_emotion_dialog_cnt,
        -- 会话量统计-自定义质检
        sum((
                length(rule_stats_id)
                +
                length(arrayFilter(x->x<0, xrule_stats_score))
                +
                length(arrayFilter(x->x<0, top_xrules_score))
            )!=0
        ) AS custom_subtract_score_dialog_cnt,
        sum((
                length(rule_add_stats_id)
                +
                length(arrayFilter(x->x>0, xrule_stats_score))
                +
                length(arrayFilter(x->x>0, top_xrules_score))
            )!=0
        ) AS custom_add_score_dialog_cnt,
        -- 会话量统计-人工质检
        sum(length(mark_ids)!=0) AS manual_marked_dialog_cnt, -- 被人工质检会话量
        sum(arrayExists((x)->x>0, tag_score_stats_score)) AS manual_subtract_score_dialog_cnt, -- 人工质检扣分会话量
        sum(arrayExists((x)->x>0, tag_score_add_stats_score)) AS manual_add_score_dialog_cnt -- 人工质检加分会话量
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) = {ds_nodash}
    GROUP BY day, platform, seller_nick, snick
) AS dws_snick_stat
GLOBAL LEFT JOIN (
    -- 获取维度数据快照
    SELECT
        snick, employee_id, employee_name, department_id, department_name
    FROM xqc_dim.snick_full_info_all
    WHERE day = {snapshot_ds_nodash}
) AS dim_snick_department
USING(snick)