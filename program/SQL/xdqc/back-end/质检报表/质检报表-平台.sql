-- 平台维度
-- ods.qc_statistical_all
-- ods.qc_session_count_all

-- PS: 页面左下角的相关记录数,是重新执行了 SELECT COUNT(*) AS count FROM (质检报表查询) 实现的
-- if req.DepartmentId == ""
select
    platform, -- 平台
    '全部' as department_name, -- 分组名
    count(distinct shop_name) as shop_count, -- 店铺数
    round(avg(if(avg_score is null, 0, avg_score)), 2) as avg_score, -- 平均分
    sum(employee_count) as employee_count, -- 客服人数
    sum(all_count) as all_count, -- 会话总量
    sum(ai_count) as ai_count, -- AI质检量
    sum(ai_abnormal_count) as ai_abnormal_count, -- AI质检异常/扣分会话量
    sum(ai_excellent_count) as ai_excellent_count,  -- AI质检加分会话量
    sum(read_count) as read_count, -- 人工抽检量/人工质检会话数量
    round(
        (0.9604 * all_count) /(0.0025 * all_count + 0.9604),
        0
    ) as suggest_count, -- 建议抽检量/建议人工质检量
    concat(
        CAST(
            round((read_count * 100 / all_count), 2),
            'String'
        ),
        '%'
    ) as check_rate, -- 抽检比例/人工质检会话量占比
    sum(tag_score_count) as tag_score_count, -- 人工质检扣分会话量
    sum(tag_add_score_count) as tag_add_score_count, -- 人工质检加分会话量
    concat(
        CAST(
            round((ai_abnormal_count * 100 / all_count), 2),
            'String'
        ),
        '%'
    ) AS ai_abnormal_rate, -- AI质检扣分会话占比
    concat(
        CAST(
            round((tag_score_count * 100 / all_count), 2),
            'String'
        ),
        '%'
    ) AS tag_score_rate -- 人工质检扣分会话占比
from (
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
            sum(tag_score_stats_count) as tag_score_count,
            sum(tag_score_add_stats_count) as tag_add_score_count
        from ods.qc_statistical_all
        where platform = '%s'
            and company_id = '%s'
            and date >= %d
            and date <= %d -- (sess.Platform, sess.CompanyId.Hex(), req.StartDate, req.EndDate)
        group by company_id,
            seller_nick,
            platform,
            department_name,
            department_id
    ) as qc
    left join (
        select company_id,
            (
                sum(session_count) * 100 + sum(ai_add_score) - sum(ai_subtract_score)
            ) / sum(session_count) as avg_score,
            sum(rule_score_count) as rule_score_count,
            sum(rule_add_score_count) as rule_add_score_count
        from ods.qc_session_count_all
        where platform = '%s'
            and company_id = '%s'
            and date >= %d
            and date <= %d -- (sess.Platform, sess.CompanyId.Hex(), req.StartDate, req.EndDate)
        group by company_id
    ) as score using(company_id)
group by platform
order by %s %s -- (sortKey, sortType)
limit %d offset %d -- (req.PageSize, (req.CurrentPage-1)*req.PageSize)

-- if req.DepartmentId != ""
select platform,
    department_name,
    department_id,
    count(distinct shop_name) as shop_count,
    sum(employee_count) as employee_count,
    sum(all_count) as all_count,
    sum(ai_count) as ai_count,
    sum(ai_abnormal_count) as ai_abnormal_count,
    sum(ai_excellent_count) as ai_excellent_count,
    sum(read_count) as read_count,
    round(
        (0.9604 * all_count) /(0.0025 * all_count + 0.9604),
        0
    ) as suggest_count,
    concat(
        CAST(
            round((read_count * 100 / all_count), 2),
            'String'
        ),
        '%'
    ) as check_rate,
    sum(tag_score_count) as tag_score_count,
    sum(tag_add_score_count) as tag_add_score_count
from (
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
            sum(tag_score_stats_count) as tag_score_count,
            sum(tag_score_add_stats_count) as tag_add_score_count
        from ods.qc_statistical_all
        where platform = '%s'
            and has(%s, department_id)
            and company_id = '%s'
            and date >= %d
            and date <= %d -- (sess.Platform, service.BuildSqlArrayById(departmentIds), sess.CompanyId.Hex(), req.StartDate, req.EndDate)
        group by company_id,
            seller_nick,
            platform,
            department_name,
            department_id
    ) as qc
    left join (
        select company_id,
            (
                sum(session_count) * 100 + sum(ai_add_score) - sum(ai_subtract_score)
            ) / sum(session_count) as avg_score,
            sum(rule_score_count) as rule_score_count,
            sum(rule_add_score_count) as rule_add_score_count
        from ods.qc_session_count_all
        where platform = '%s'
            and has(%s, department_id)
            and company_id = '%s'
            and date >= %d
            and date <= %d -- (sess.Platform, service.BuildSqlArrayById(departmentIds), sess.CompanyId.Hex(), req.StartDate, req.EndDate)
        group by company_id
    ) as score using(company_id)
group by platform,
    department_name,
    department_id
order by %s %s -- (sortKey, sortType)
limit %d offset %d -- (req.PageSize, (req.CurrentPage-1)*req.PageSize)