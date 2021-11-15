-- 店铺详情 - 客服详情
-- ods.qc_statistical_employee_all
-- ods.qc_session_count_all

-- 页面左下角的相关记录数
-- ods.qc_statistical_employee_all
select count(*) as count
from(
        select employee_id,
            arrayReduce('groupUniqArray', flatten(groupArray(mark_list))) as mark_list
        from ods.qc_statistical_employee_all
        where date <= %d
            and date >= %d
            and company_id = '%s'
            and has(%s, department_id)
            and has(%s, employee_id) -- (req.EndDate, req.StartDate, sess.CompanyId.Hex(),BuildSqlArrayById(departmentIds),BuildSqlArrayByStr(req.EmployeeId))
        group by employee_id,
            employee_name,
            department_id,
            department_name
    )
where hasAny(mark_list, %s) -- BuildSqlArrayByStr(req.AccountName)

-- 客服详情信息
-- ods.qc_session_count_all
-- ods.qc_statistical_employee_all
select
    employee_id, -- 客服ID
    employee_name, -- 客服姓名
    round(avg_score, 2) as avg_score, -- 平均分
    department_id, -- 分组ID
    department_name, -- 分组名
    all_count, -- 总会话量
    ai_count, -- AI质检量
    ai_abnormal_count, -- AI异常会话量/AI质检扣分会话总量
    ai_excellent_count, -- AI加分会话量/AI质检加分会话总量
    read_count, -- 人工抽检量/人工质检会话数量
    concat(
        CAST(
            round((tag_score_count * 100 / all_count), 2),
            'String'
        ),
        '%'
    ) AS tag_score_rate, -- 人工扣分会话占比/人工质检扣分会话占比
    concat(
        CAST(
            round((ai_abnormal_count * 100 / all_count), 2),
            'String'
        ),
        '%'
    ) AS ai_abnormal_rate, -- AI扣分会话比例/AI质检扣分会话占比
    tag_score_count, -- 人工质检扣分会话量
    tag_add_score_count, -- 人工质检加分会话量
    suggest_count, -- 建议抽检量/建议人工质检会话数量
    check_rate, -- 抽检比例/人工质检会话量占比
    replaceRegexpAll(toString(mark_list), '(\\\\[|\\\\]|'')', '') as mark_lists -- 人工抽检名单
from(
        select employee_id,
            employee_name,
            department_id,
            department_name,
            sum(all_count) as all_count,
            sum(ai_count) as ai_count,
            sum(ai_abnormal_count) as ai_abnormal_count,
            sum(ai_excellent_count) as ai_excellent_count,
            sum(read_count) as read_count,
            sum(tag_score_count) as tag_score_count,
            sum(tag_add_score_count) as tag_add_score_count,
            round((0.9604 * all_count) /(0.0025 * all_count + 0.9604), 0) as suggest_count,
            concat(
                CAST(round((read_count * 100 / all_count), 2), 'String'),
                '%'
            ) as check_rate,
            check_rate,
            arrayReduce('groupUniqArray', flatten(groupArray(mark_list))) as mark_list
        from ods.qc_statistical_employee_all
        where date <= %d
            and date >= %d
            and company_id = '%s'
            and has(%s, department_id)
            and has(%s, employee_id) -- (req.EndDate, req.StartDate, sess.CompanyId.Hex(),BuildSqlArrayById(departmentIds),BuildSqlArrayByStr(req.EmployeeId))
        group by employee_id,
            employee_name,
            department_id,
            department_name
    )
    left join (
        SELECT company_id,
            employee_id,
            employee_name,
            (
                sum(session_count) * 100 + sum(ai_add_score) - sum(ai_subtract_score)
            ) / sum(session_count) AS avg_score
        FROM ods.qc_session_count_all
        where date <= %d
            and date >= %d
            and company_id = '%s'
            and has(%s, department_id)
            and has(%s, employee_id) -- (req.EndDate, req.StartDate, sess.CompanyId.Hex(),BuildSqlArrayById(departmentIds),BuildSqlArrayByStr(req.EmployeeId))
        GROUP BY company_id,
            employee_id,
            employee_name
    ) AS score USING(employee_id, employee_name)
where hasAny(mark_list, %s) -- BuildSqlArrayByStr(req.AccountName)
order by %s %s -- (req.OrderKey, req.OrderType)
limit %d offset %d -- (req.PageSize, req.PageSize*(req.CurrentPage-1))
