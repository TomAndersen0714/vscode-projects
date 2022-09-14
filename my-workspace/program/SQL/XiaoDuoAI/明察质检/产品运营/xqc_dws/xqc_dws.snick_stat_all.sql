-- xqc_dws.snick_stat_all
ALTER TABLE xqc_dws.snick_stat_local ON CLUSTER cluster_3s_2r
ADD COLUMN tagged_dialog_cnt Int64 AFTER `dialog_cnt`,
ADD COLUMN ai_tagged_dialog_cnt Int64 AFTER `tagged_dialog_cnt`,
ADD COLUMN custom_tagged_dialog_cnt Int64 AFTER `ai_tagged_dialog_cnt`,
ADD COLUMN manual_tagged_dialog_cnt Int64 AFTER `custom_tagged_dialog_cnt`

ALTER TABLE xqc_dws.snick_stat_all ON CLUSTER cluster_3s_2r
ADD COLUMN tagged_dialog_cnt Int64 AFTER `dialog_cnt`,
ADD COLUMN ai_tagged_dialog_cnt Int64 AFTER `tagged_dialog_cnt`,
ADD COLUMN custom_tagged_dialog_cnt Int64 AFTER `ai_tagged_dialog_cnt`,
ADD COLUMN manual_tagged_dialog_cnt Int64 AFTER `custom_tagged_dialog_cnt`

-- INSERT INTO
INSERT INTO xqc_dws.snick_stat_all
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
    -- 会话量统计-总计-打标会话量
    tagged_dialog_cnt,
    -- 会话量统计-总计-AI质检打标会话量
    ai_tagged_dialog_cnt,
    -- 会话量统计-总计-自定义质检打标会话量
    custom_tagged_dialog_cnt,
    -- 会话量统计-总计-人工质检打标会话量
    manual_tagged_dialog_cnt,

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
        uniqExact(_id) AS dialog_cnt,
        -- 会话量统计-总计-打标会话量
        uniqExactIf(
            _id,
            (
                length(arrayFilter(x->x!='', abnormals_rule_id)) + length(arrayFilter(x->x!='', excellents_rule_id)) +
                length(rule_stats_id) + length(xrule_stats_id) + length(top_xrules_id) +
                length(tag_score_add_stats_id) + length(tag_score_stats_id)
            )!=0
        ) AS tagged_dialog_cnt,
        -- 会话量统计-总计-AI质检打标会话量
        uniqExactIf(
            _id, (length(arrayFilter(x->x!='', abnormals_rule_id)) + length(arrayFilter(x->x!='', excellents_rule_id)))!=0
        ) AS ai_tagged_dialog_cnt,
        -- 会话量统计-总计-自定义质检打标会话量
        uniqExactIf(
            _id, (length(rule_stats_id) + length(xrule_stats_id) + length(top_xrules_id))!=0
        ) AS custom_tagged_dialog_cnt,
        -- 会话量统计-总计-人工质检打标会话量
        uniqExactIf(
            _id, (length(tag_score_add_stats_id) + length(tag_score_stats_id))!=0
        ) AS manual_tagged_dialog_cnt,
        -- 会话量统计-扣分会话量
        uniqExactIf(_id, score>0) AS subtract_score_dialog_cnt,
        -- 会话量统计-加分会话量
        uniqExactIf(_id, score_add>0) AS add_score_dialog_cnt,

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
