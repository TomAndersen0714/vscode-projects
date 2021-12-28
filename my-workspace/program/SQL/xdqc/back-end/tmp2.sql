select employee_id,
    employee_name,
    sum(session_count) as total_count,
    0 as total_check,
    sum(ai_subtract_score) as abnormal_score,
    sum(subtract_score_count) / sum(session_count) as abnormal_rate,
    sum(ai_subtract_score) - sum(manual_subtract_score) - sum(rule_score) as ai_abnormal_score,
    sum(manual_subtract_score) as human_abnormal_score,
    0 as human_total_check,
    0 as average_check,
    sum(rule_score) AS user_rule_score,
    round(
        (
            sum(session_count) * 100 + sum(ai_add_score) - sum(ai_subtract_score)
        ) / sum(session_count),
        2
    ) AS avg_score
from ods.qc_session_count_all
where date >= 1640016000 and date < 1640102399
    and shop_name in ['方太官方旗舰店']
    and platform = 'tb'
    and employee_name != ''
group by employee_id,
    employee_name
order by avg_score desc