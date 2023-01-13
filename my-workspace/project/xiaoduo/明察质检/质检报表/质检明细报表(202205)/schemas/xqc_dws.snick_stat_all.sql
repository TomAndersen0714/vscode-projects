-- 表2:
--  统计维度: 天/平台/店铺/子账号-子账号分组-客服姓名
--  统计数据: 质检报表中的会话总量/各个质检类别的分值/各个类别扣分会话量等等
--  PS: 任何下层维度的统计指标之间存在交集, 在向其他维度聚合时, 需要和产品确定是否取并集, 还是直接相加, 默认是直接相加

-- xqc_dws
CREATE DATABASE xqc_dws ON CLUSTER cluster_3s_2r
ENGINE = Ordinary

-- xqc_dws.snick_stat_local
CREATE TABLE xqc_dws.snick_stat_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `seller_nick` String,
    `snick` String,
    `employee_id` String,
    `employee_name` String,
    `department_id` String,
    `department_name` String,
    `subtract_score_sum` Int64,
    `add_score_sum` Int64,
    `ai_subtract_score_sum` Int64,
    `ai_add_score_sum` Int64,
    `custom_subtract_score_sum` Int64,
    `custom_add_score_sum` Int64,
    `manual_subtract_score_sum` Int64,
    `manual_add_score_sum` Int64,
    `dialog_cnt` Int64,
    `manual_marked_dialog_cnt` Int64,
    `ai_subtract_score_dialog_cnt` Int64,
    `ai_add_score_dialog_cnt` Int64,
    `custom_subtract_score_dialog_cnt` Int64,
    `custom_add_score_dialog_cnt` Int64,
    `manual_subtract_score_dialog_cnt` Int64,
    `manual_add_score_dialog_cnt` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY `day`
ORDER BY (platform, seller_nick, snick)
SETTINGS storage_policy = 'rr', index_granularity = 8192

-- xqc_dws.snick_stat_all
-- DROP TABLE xqc_dws.snick_stat_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dws.snick_stat_all ON CLUSTER cluster_3s_2r
AS xqc_dws.snick_stat_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dws', 'snick_stat_local', rand())


-- 质检结果总览-子账号维度
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
    -- 获取最新版本的维度数据(T+1)
    SELECT
        snick, employee_id, employee_name, department_id, department_name
    FROM (
        SELECT snick, employee_id, employee_name, department_id
        FROM (
            -- 查询对应企业-平台的所有子账号及其部门ID, 不论其是否绑定员工
            SELECT snick, department_id, employee_id
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
        ) AS snick_info
        GLOBAL LEFT JOIN (
            SELECT
                _id AS employee_id, username AS employee_name
            FROM ods.xinghuan_employee_all
            WHERE day = toYYYYMMDD(yesterday())
        ) AS employee_info
        USING(employee_id)
    ) AS snick_info
    GLOBAL LEFT JOIN (
        SELECT
            _id AS department_id,
            full_name AS department_name
        FROM xqc_dim.snick_department_full_all
        WHERE day = toYYYYMMDD(yesterday())
    ) AS department_info
    USING (department_id)
) AS dim_snick_department
USING(snick)
