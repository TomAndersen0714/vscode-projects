-- 店铺
-- ods.qc_statistical_all
-- ods.qc_session_count_all

-- PS: 页面左下角的相关记录数,是重新执行了 SELECT COUNT(*) AS count FROM (质检报表查询) 实现的
-- if req.DepartmentId == ""
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


