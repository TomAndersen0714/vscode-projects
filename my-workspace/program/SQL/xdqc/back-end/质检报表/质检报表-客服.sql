-- COUNT SQL
select count(*) as count
from(
        select employee_id,
            arrayReduce('groupUniqArray', flatten(groupArray(mark_list))) as mark_list
        from ods.qc_statistical_employee_all
        where date>=1640707200 AND date<=1640793599
        and company_id = '61602afd297bb79b69c06118'
        and employee_id = '6180e523905445c2b9b90730'
        group by employee_id,
            employee_name,
            department_id,
            department_name
    )

-- SHOW SQL
select employee_id,
    employee_name,
    round(avg_score, 2) as avg_score,
    department_id,
    department_name,
    all_count,
    ai_count,
    ai_abnormal_count,
    ai_excellent_count,
    read_count,
    concat(
        CAST(
            round((tag_score_count * 100 / all_count), 2),
            'String'
        ),
        '%'
    ) AS tag_score_rate,
    concat(
        CAST(
            round((ai_abnormal_count * 100 / all_count), 2),
            'String'
        ),
        '%'
    ) AS ai_abnormal_rate,
    tag_score_count,
    tag_add_score_count,
    suggest_count,
    check_rate,
    replaceRegexpAll(toString(mark_list), '(\\\\[|\\\\]|'')', '') as mark_lists
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
        where date >= 1635609600 AND date <= 1636214399
            and company_id = '61602afd297bb79b69c06118'
            and employee_id = '6180e523905445c2b9b90730'
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
        where date >= 1635609600 AND date <= 1636214399
            and company_id = '61602afd297bb79b69c06118'
            and employee_id = '6180e523905445c2b9b90730'
        GROUP BY company_id,
            employee_id,
            employee_name
    ) AS score USING(employee_id, employee_name)