insert into ods.qc_statistical_department_all
select a.`date`,
    b.company_id,
    b.department_id,
    b.department_name,
    sum(sessionc_count) as all_count,
    sum(qc_count) as ai_count,
    sum(abnormals_count) as ai_abnormal_count,
    sum(excellents_count) as ai_excellent_count,
    sum(read_mark_count) as read_count,
    sum(if(tag_score_stats_count > 0, 1, 0)) as tag_score_count,
    sum(if(tag_score_add_stats_count > 0, 1, 0)) as tag_add_score_count,
    round((0.9604 * all_count) /(0.0025 * all_count + 0.9604), 0) as suggest_count,
    concat(
        CAST(
            round(
                (sum(if(read_mark_count > 0, 1, 0)) * 100 / all_count),
                2
            ),
            'String'
        ),
        '%'
    ) as check_rate,
    arrayReduce('groupUniqArray', flatten(groupArray(mark_list))) as mark_list
from (
        SELECT a.company_id AS company_id,
            a._id AS department_id,
            a.name AS department_name,
            b.employee_id AS employee_id,
            b.employee_name AS employee_name,
            b.snick AS snick
        FROM (
                select *
                from ods.xinghuan_department_all
                where day = { ds_nodash }
            ) AS a GLOBAL
            LEFT JOIN (
                SELECT a._id AS employee_id,
                    b.department_id AS department_id,
                    a.username AS employee_name,
                    b.snick AS snick
                FROM(
                        select *
                        from ods.xinghuan_employee_all
                        where day = { ds_nodash }
                    ) AS a GLOBAL
                    RIGHT JOIN (
                        select *
                        from ods.xinghuan_employee_snick_all
                        where day = { ds_nodash }
                            and platform = 'tb'
                    ) AS b ON a._id = b.employee_id
            ) AS b ON a._id = b.department_id
    ) b GLOBAL
    left join ods.qc_statistical_all a on a.snick = b.snick
where a.`date` = toDate('{ds}')
group by a.`date`,
    b.company_id,
    b.department_id,
    b.department_name
order by ai_abnormal_count desc