-- 后端查询SQL
-- 客服服务质量排行
select 
    'server' as type, 
    employee_id, 
    employee_name, 
    sum(session_count) as total_count, 
    0 as total_check, 
    sum(ai_subtract_score) as abnormal_score, 
    sum(subtract_score_count) / sum(session_count)  as abnormal_rate, 
    sum(ai_subtract_score)- sum(manual_subtract_score)-sum(rule_score) as ai_abnormal_score, 
    sum(manual_subtract_score) as human_abnormal_score, 
    0 as human_total_check, 
    0 as average_check, 
    sum(rule_score) AS user_rule_score, 
    round((sum(session_count) *100 +sum(ai_add_score) -sum(ai_subtract_score)) /sum(session_count),2) AS avg_score 
from ods.qc_session_count_all 
where date >= %d 
and date < %d 
and shop_name in %s 
and employee_name != '' 
group by employee_id, employee_name 
order by avg_score desc limit 10 
/* 
(
    startDate, endDate, shopStr
)
*/

union all

-- 客服被检量排行
-- PS: 仅统计客服被抽检的那几天的数据, 即 manual_qc_count != 0
select 
    'server_read_mark' as type, 
    employee_id, 
    employee_name, 
    sum(session_count) as total_count, 
    0 as total_check, 
    0 as abnormal_score, 
    0 as abnormal_rate, 
    0 as ai_abnormal_score, 
    0 as human_abnormal_score, 
    sum(manual_qc_count) as human_total_check, 
    sum(manual_qc_count)/if(dateDiff('day', toDate(%d), toDate(%d))=0,1, dateDiff('day', toDate(%d), toDate(%d))) as average_check, 
    0 as user_rule_score,
    0 as avg_score 
from ods.qc_session_count_all
where date >= %d 
and date < %d 
and shop_name in %s 
and employee_name != '' 
and manual_qc_count != 0 
group by employee_id, employee_name 
order by human_total_check desc 
limit 10 
/* 
(
    startDate, endDate, startDate, endDate,
    startDate, endDate, shopStr
)
*/

union all

-- 质检人员质检量排行
select 
    'read_mark' as type, 
    account_id as employee_id, 
    username as employee_name, 
    0 as total_count, 
    count(1) as total_check, 
    0 as abnormal_score, 
    0 as abnormal_rate, 
    0 as ai_abnormal_count, 
    0 as human_abnormal_count, 
    0 as human_total_check, 
    count(1)/if(dateDiff('day', toDate(%d), toDate(%d))=0,1, dateDiff('day', toDate(%d), toDate(%d))) as average_check, 
    0 as user_rule_score, 
    0 as avg_score 
from ods.qc_read_mark_detail_all 
where username != '' 
and date >= %d 
and date < %d 
and shop_name in %s 
and employee_name != '' 
group by account_id,username 
order by total_check desc 
limit 10

/* 
(
    startDate, endDate, startDate, endDate,
    startDate, endDate, shopStr
)
*/