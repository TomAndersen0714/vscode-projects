insert into ods.qc_statistical_department_all
select a.`date`,
    a.company_id,
    a.platform,
    a.department_id,
    a.department_name,
    sum(a.sessionc_count) as all_count,
    sum(a.qc_count) as ai_count,
    sum(a.abnormals_count) as ai_abnormal_count,
    sum(a.excellents_count) as ai_excellent_count,
    sum(a.read_mark_count) as read_count,
    sum(if(a.tag_score_stats_count > 0, 1, 0)) as tag_score_count,
    sum(if(a.tag_score_add_stats_count > 0, 1, 0)) as tag_add_score_count,
    round((0.9604 * all_count) /(0.0025 * all_count + 0.9604), 0) as suggest_count,
    concat(
        CAST(
            round(
                (sum(if(a.read_mark_count > 0, 1, 0)) * 100 / all_count),
                2
            ),
            'String'
        ),
        '%'
    ) as check_rate,
    arrayReduce('groupUniqArray', flatten(groupArray(a.mark_list))) as mark_list
from ods.qc_statistical_all AS a
where a.`date` = toDate('{ds}')
group by a.`date`,
    a.platform,
    a.company_id,
    a.department_id,
    a.department_name
order by ai_abnormal_count desc