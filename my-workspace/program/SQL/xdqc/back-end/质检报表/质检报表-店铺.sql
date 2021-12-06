-- 店铺
-- ods.qc_statistical_all
-- ods.qc_session_count_all

-- PS: 页面左下角的相关记录数,是重新执行了 SELECT COUNT(*) AS count FROM (质检报表查询) 实现的
-- if req.DepartmentId == ""
select
    shop_name, -- 店铺名称
    platform, -- 平台
    department_name, -- 分组名称
    department_id, -- 分组ID
    employee_count, -- 客服人数
    all_count, -- 总会话量
    ai_count, -- AI质检量/AI质检会话总量
    ai_abnormal_count, -- AI异常会话量/AI质检扣分会话量
    ai_excellent_count, -- AI加分会话量/AI质检加分会话量
    read_count, -- 人工抽检量/人工质检会话数量
    suggest_count, -- 建议抽检量/建议人工质检会话数量
    check_rate, -- 抽检比例/人工质检会话量占比
    tag_score_count, -- 人工质检扣分会话量
    tag_add_score_count, -- 人工质检加分会话量
    round(avg_score, 2) as avg_score, -- 平均分
    concat(
        CAST(
            round((ai_abnormal_count * 100 / all_count), 2),
            'String'
        ),
        '%'
    ) AS ai_abnormal_rate, -- AI扣分会话比例/AI质检扣分会话占比
    concat(
        CAST(
            round((tag_score_count * 100 / all_count), 2),
            'String'
        ),
        '%'
    ) AS tag_score_rate -- 人工扣分会话占比/人工质检扣分会话占比
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
        where platform = '%s'
            and company_id = '%s'
            and date >= %d
            and date <= %d -- (sess.Platform, sess.CompanyId.Hex(), req.StartDate, req.EndDate)
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
        where platform = '%s'
            and company_id = '%s'
            and date >= %d
            and date <= %d -- (sess.Platform, sess.CompanyId.Hex(), req.StartDate, req.EndDate)
        group by company_id,
            shop_name,
            platform,
            department_name,
            department_id
    ) as score 
    using(company_id, department_id)
order by %s %s -- (sortKey, sortType)
limit %d offset %d -- (req.PageSize, (req.CurrentPage-1)*req.PageSize)

-- if req.DepartmentId != ""
select shop_name,
    platform,
    department_name,
    department_id,
    employee_count,
    all_count,
    ai_count,
    ai_abnormal_count,
    ai_excellent_count,
    read_count,
    suggest_count,
    check_rate,
    tag_score_count,
    tag_add_score_count,
    round(avg_score, 2) as avg_score,
    concat(
        CAST(
            round((ai_abnormal_count * 100 / all_count), 2),
            'String'
        ),
        '%'
    ) AS ai_abnormal_rate,
    concat(
        CAST(
            round((tag_score_count * 100 / all_count), 2),
            'String'
        ),
        '%'
    ) AS tag_score_rate
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
            round((0.9604 * all_count) /(0.0025 * all_count + 0.9604), 0) as suggest_count,
            concat(
                CAST(round((read_count * 100 / all_count), 2), 'String'),
                '%'
            ) as check_rate,
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
    ) as emp
    left join (
        SELECT company_id,
            shop_name,
            platform,
            department_id,
            department_name,
            (
                sum(session_count) * 100 + sum(ai_add_score) - sum(ai_subtract_score)
            ) / sum(session_count) AS avg_score
        FROM ods.qc_session_count_all
        where platform = '%s'
            and has(%s, department_id)
            and company_id = '%s'
            and date >= %d
            and date <= %d -- (sess.Platform, service.BuildSqlArrayById(departmentIds), sess.CompanyId.Hex(), req.StartDate, req.EndDate)
        group by company_id,
            shop_name,
            platform,
            department_name,
            department_id
    ) as score using(company_id, department_id)
order by %s %s -- (sortKey, sortType)
limit %d offset %d -- (req.PageSize, (req.CurrentPage-1)*req.PageSize)