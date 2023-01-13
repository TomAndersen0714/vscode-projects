-- 店铺
-- ods.qc_statistical_all
-- ods.qc_session_count_all


-- 统计维度: 子账号分组, 下钻维度: 会话
SELECT
    seller_nick AS `店铺名称`,
    department_id,
    department_name AS `分组`,
    count(distinct snick) AS `客服人数`,
    count(1) AS `总会话量`,
    round((`总会话量`*100 + sum(score_add)- sum(score))/`总会话量`,2) AS `平均分`,
    `总会话量` AS `AI质检量`,
    sum(arraySum(abnormals_count)!=0) AS `AI异常会话量`,
    concat(
        CAST(
            round((`AI异常会话量` * 100 / `总会话量`), 2),
            'String'
        ),
        '%'
    ) AS `AI扣分会话比例`,
    sum(arraySum(excellents_count)!=0) AS `AI加分会话量`,
    round((0.9604 * `总会话量`) /(0.0025 * `总会话量` + 0.9604), 0) as `建议抽检量`,
    sum(length(mark_ids)!=0) AS `人工抽检量`,
    concat(
        CAST(round((`人工抽检量` * 100 / `总会话量`), 2), 'String'),
        '%'
    ) as `抽检比例`,
    sum(length(tag_score_stats_id)!=0) `人工质检扣分会话量`,
    concat(
        CAST(
            round((`人工质检扣分会话量` * 100 / `总会话量`), 2),
            'String'
        ),
        '%'
    ) AS `人工扣分会话比例`,
    sum(length(tag_score_add_stats_id)!=0) `人工质检加分会话量`
FROM (
    SELECT *
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
    AND platform = '{{platform}}'
    AND snick GLOBAL IN (
        SELECT distinct snick
        FROM ods.xinghuan_employee_snick_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
        AND company_id = '{{ company_id }}'
    )
) AS dialog_info
GLOBAL LEFT JOIN (
    -- 子账号和部门映射关系
    -- ods.xinghuan_department_all
    -- ods.xinghuan_employee_snick_all
    SELECT
        snick,
        department_id,
        department_name
    FROM (
        -- 查找最新的分组
        SELECT DISTINCT
            _id AS department_id,
            name AS department_name
        FROM ods.xinghuan_department_all
        WHERE day = toYYYYMMDD(today()-1)
        AND company_id = '{{ company_id }}'
    ) AS department_info
    GLOBAL INNER JOIN (
        -- 包括未绑定员工的子账号
        SELECT DISTINCT
            snick,
            department_id
        FROM ods.xinghuan_employee_snick_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
        AND platform = '{{platform}}'
    ) AS snick_info
    USING department_id
)
USING snick
GROUP BY seller_nick, department_id, department_name



-- 原始
select
    shop_name AS `店铺名称`,
    platform AS `平台`, 
    department_name AS `分组`,
    employee_count AS `客服人数`,
    all_count AS `总会话量`,
    round(avg_score, 2) AS `平均分`,
    ai_count AS `AI质检量`,
    ai_abnormal_count AS `AI异常会话量`,
    concat(
        CAST(
            round((ai_abnormal_count * 100 / all_count), 2),
            'String'
        ),
        '%'
    ) AS `AI扣分会话比例`,
    ai_excellent_count AS `AI加分会话量`,
    suggest_count AS `建议抽检量`,
    read_count AS `人工抽检量`,
    check_rate AS `抽检比例`,
    tag_score_count AS `人工质检扣分会话量`,
    concat(
        CAST(
            round((tag_score_count * 100 / all_count), 2),
            'String'
        ),
        '%'
    ) AS `人工扣分会话比例`,
    tag_add_score_count AS `人工质检加分会话量`
from (
        -- 统计各分组的各种质检项目触发次数等
        select company_id,
            seller_nick as shop_name,
            platform,
            department_name,
            department_id,
            count(distinct snick) as employee_count,
            sum(sessionc_count) as all_count,
            sum(qc_count) as ai_count,
            sum(abnormals_count) as ai_abnormal_count,
            sum(excellents_count) as ai_excellent_count,
            sum(read_mark_count) as read_count,
            round((0.9604 * all_count) /(0.0025 * all_count + 0.9604), 0) as suggest_count,
            concat(
                CAST(round((read_count * 100 / all_count), 2), 'String'),
                '%'
            ) as check_rate,
            sum(tag_score_stats_count) as tag_score_count,
            sum(tag_score_add_stats_count) as tag_add_score_count
        from ods.qc_statistical_all
        where platform = 'tb'
            and company_id = '{{company_id}}'
            and toYYYYMMDD(date) BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
        group by company_id,
            seller_nick,
            platform,
            department_name,
            department_id
    ) as emp
    left join (
        -- 统计各个分组的平均分
        SELECT company_id,
            shop_name,
            platform,
            department_id,
            department_name,
            (
                sum(session_count) * 100 + sum(ai_add_score) - sum(ai_subtract_score)
            ) / sum(session_count) AS avg_score
        FROM ods.qc_session_count_all
        where platform = 'tb'
            and company_id = '{{company_id}}'
            and toYYYYMMDD(date) BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
        group by company_id,
            shop_name,
            platform,
            department_name,
            department_id
    ) as score 
    using(shop_name, platform, department_name, department_id)
-- HAVING ai_abnormal_count!=0
ORDER BY shop_name,department_name


