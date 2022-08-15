-- 统计维度: 天/平台/主账号/子账号/质检项一级分组
-- 度量: 扣分会话数, 加分会话数
-- 质检项分组统计

-- t1
-- PS: 此处需要JOIN 3次来获取分组的完整路径, 因为分组树高为4
-- qc_norm_id = '6245ff912e61eef7f95c1962'
INSERT INTO {sink_tbl}
SELECT
    -- parent_group_id 全为空, 即当前树型结构层次遍历完毕
    level_2_3_4._id,
    level_2_3_4.create_time,
    level_2_3_4.update_time,
    level_2_3_4.company_id,
    level_2_3_4.platform,
    level_2_3_4.qc_norm_id,
    level_2_3_4.short_name AS name,
    if(
        level_1._id!='', 
        concat(level_1.short_name,'/',level_2_3_4.full_name),
        level_2_3_4.full_name
    ) AS full_name,
    (length(super_group_ids) + 1) AS level,
    level_2_3_4.parent_id AS parent_id,
    if(
        level_1._id!='', 
        arrayPushFront(level_2_3_4.super_group_ids, level_1._id),
        level_2_3_4.super_group_ids
    ) AS super_group_ids,
    {snapshot_ds_nodash} AS day
FROM (
    SELECT
        level_3_4.create_time,
        level_3_4.update_time,
        level_3_4.company_id,
        level_3_4.platform,
        level_3_4.qc_norm_id,
        level_3_4.parent_id AS parent_id,
        level_2.parent_id AS top_parent_id,
        level_3_4._id AS _id,
        level_3_4.short_name AS short_name,
        if(
            level_2._id!='', 
            concat(level_2.short_name,'/',level_3_4.full_name),
            level_3_4.full_name
        ) AS full_name,
        if(
            level_2._id!='', 
            arrayPushFront(level_3_4.super_group_ids, level_2._id),
            level_3_4.super_group_ids
        ) AS super_group_ids
    FROM (
        SELECT
            level_4.create_time,
            level_4.update_time,
            level_4.company_id,
            level_4.platform,
            level_4.qc_norm_id,
            level_4.parent_id AS parent_id,
            level_3.parent_id AS top_parent_id,
            level_4._id AS _id,
            level_4.short_name AS short_name,
            if(
                level_3._id!='', 
                concat(level_3.short_name,'/',level_4.full_name),
                level_4.full_name
            ) AS full_name,
            if(
                level_3._id!='', 
                arrayPushFront(level_4.super_group_ids, level_3._id),
                level_4.super_group_ids
            ) AS super_group_ids
        FROM (
            SELECT 
                *,
                name AS short_name,
                name AS full_name,
                parent_id AS top_parent_id,
                [] AS super_group_ids
            FROM {source_tbl}
            WHERE day = {snapshot_ds_nodash}
        ) AS level_4
        GLOBAL LEFT JOIN (
            SELECT 
                _id,
                name AS short_name,
                parent_id
            FROM {source_tbl}
            WHERE day = {snapshot_ds_nodash}
        ) AS level_3
        ON level_4.top_parent_id = level_3._id
    ) AS level_3_4
    GLOBAL LEFT JOIN (
        SELECT 
            _id,
            name AS short_name,
            parent_id
        FROM {source_tbl}
        WHERE day = {snapshot_ds_nodash}
    ) AS level_2
    ON level_3_4.top_parent_id = level_2._id
) AS level_2_3_4
GLOBAL LEFT JOIN (
    SELECT 
        _id,
        name AS short_name,
        parent_id
    FROM {source_tbl}
    WHERE day = {snapshot_ds_nodash}
) AS level_1
ON level_2_3_4.top_parent_id = level_1._id

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
        *
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
GROUP BY tag_level_1_group_id

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
GROUP BY tag_level_2_group_id

-- 统计维度: 天/平台/主账号/子账号/质检标准
-- 度量: 扣分会话数, 加分会话数, 质检会话数

-- t1
-- 通过 department_id 关联 ods.xinghuan_qc_norm_relate_all, ods.xinghuan_employee_snick_all
SELECT
    *
FROM (
    SELECT
        qc_norm_id,
        department_id
    FROM ods.xinghuan_qc_norm_relate_all
    WHERE day = {snapshot_ds_nodash}
) AS qc_norm_binding_info
GLOBAL INNER JOIN (
    SELECT
        department_id,
        platform,
        snick
    FROM ods.xinghuan_employee_snick_all
    WHERE day = {snapshot_ds_nodash}
) AS snick_info
USING(department_id)

-- t2
SELECT
    day,
    platform,
    seller_nick,
    snick_qc_norm_info.qc_norm_id AS qc_norm_id,
    snick_qc_norm_info.department_id AS department_id,
    snick,
    dialog_id,
    score,
    score_add
FROM (
    SELECT
        {ds_nodash} AS day,
        platform,
        seller_nick,
        snick,
        _id AS dialog_id,
        score,
        score_add
    FROM dwd.xdqc_dialog_all
    WHERE day = {ds_nodash}
) AS dialog_info
-- 关联并筛选经过质检的子账号和会话
GLOBAL INNER JOIN (
    SELECT
        qc_norm_id,
        department_id,
        platform,
        snick
    FROM (
        SELECT
            qc_norm_id,
            department_id
        FROM ods.xinghuan_qc_norm_relate_all
        WHERE day = {snapshot_ds_nodash}
    ) AS qc_norm_binding_info
    GLOBAL INNER JOIN (
        SELECT
            department_id,
            platform,
            snick
        FROM ods.xinghuan_employee_snick_all
        WHERE day = {snapshot_ds_nodash}
    ) AS snick_info
    USING(department_id)
) AS snick_qc_norm_info
ON dialog_info.platform = snick_info.platform
AND dialog_info.snick = snick_info.snick

-- t3

SELECT
    day,
    platform,
    seller_nick,
    qc_norm_id,
    uniqExact(dialog_id) AS dialog_cnt,
    uniqExactIf(dialog_id, score_add>0) AS add_score_dialog_cnt,
    uniqExactIf(dialog_id, score>0) AS subtract_score_dialog_cnt
FROM (
    t2
) AS dialog_snick_info
GROUP BY day, platform, seller_nick, qc_norm_id


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
    manual_marked_dialog_cnt, -- 被人工质检会话量
    -- 会话量统计-AI质检
    ai_subtract_score_dialog_cnt,
    ai_add_score_dialog_cnt,
    -- abnormal_dialog_cnt,
    -- excellent_dialog_cnt,
    -- c_emotion_dialog_cnt,
    -- s_emotion_dialog_cnt,
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
        -- 会话量统计-加分会话量
        uniqExactIf(dialog_id, score_add>0) AS add_score_dialog_cnt,
        -- 会话量统计-扣分会话量
        uniqExactIf(dialog_id, score>0) AS subtract_score_dialog_cnt,

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
    FROM (
        SELECT snick, employee_id, employee_name, department_id
        FROM (
            -- 查询对应企业-平台的所有子账号及其部门ID, 不论其是否绑定员工
            SELECT snick, department_id, employee_id
            FROM ods.xinghuan_employee_snick_all
            WHERE day = {snapshot_ds_nodash}
        ) AS snick_info
        GLOBAL LEFT JOIN (
            SELECT
                _id AS employee_id, username AS employee_name
            FROM ods.xinghuan_employee_all
            WHERE day = {snapshot_ds_nodash}
        ) AS employee_info
        USING(employee_id)
    ) AS snick_info
    GLOBAL LEFT JOIN (
        SELECT
            _id AS department_id,
            full_name AS department_name
        FROM xqc_dim.snick_department_full_all
        WHERE day = {snapshot_ds_nodash}
    ) AS department_info
    USING (department_id)
) AS dim_snick_department
USING(snick)