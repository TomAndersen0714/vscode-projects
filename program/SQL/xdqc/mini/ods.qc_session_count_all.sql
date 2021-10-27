-- ods.qc_session_count_all
insert into ods.qc_session_count_all
select toDate('{ds}') as `date`,
    a.platform,
    company_id,
    company_name,
    department_id,
    department_name,
    employee_id,
    employee_name,
    a.`group`,
    a.shop_name,
    a.snick,
    a.session_count,
    a.add_score_count,
    a.subtract_score_count,
    a.manual_qc_count,
    a.ai_abnormal_count,
    a.manual_abnormal_count,
    a.ai_add_score,
    a.manual_add_score,
    a.ai_subtract_score,
    a.manual_subtract_score,
    a.ai_add_score_count,
    a.manual_add_score_count,
    a.ai_subtract_score_count,
    a.manual_subtract_score_count,
    a.rule_score_count,
    a.rule_score,
    a.rule_add_score_count,
    a.rule_add_score,
    length(b.dialog_array),
    b.dialog_array
from (
        select session_info.`date`,
            session_info.platform as platform,
            dim_info.company_id as company_id,
            '' as company_name,
            dim_info.department_id as department_id,
            dim_info.department_name as department_name,
            dim_info.employee_id as employee_id,
            dim_info.employee_name as employee_name,
            session_info.group as group,
            session_info.shop_name as shop_name,
            session_info.snick as snick,
            session_info.session_count as session_count,
            session_info.add_score_count as add_score_count,
            session_info.subtract_score_count as subtract_score_count,
            session_info.manual_qc_count as manual_qc_count,
            session_info.ai_abnormal_count as ai_abnormal_count,
            session_info.manual_abnormal_count as manual_abnormal_count,
            session_info.ai_add_score as ai_add_score,
            session_info.manual_add_score as manual_add_score,
            session_info.ai_subtract_score as ai_subtract_score,
            session_info.manual_subtract_score as manual_subtract_score,
            session_info.ai_add_score_count as ai_add_score_count,
            session_info.manual_add_score_count as manual_add_score_count,
            session_info.ai_subtract_score_count as ai_subtract_score_count,
            session_info.manual_subtract_score_count as manual_subtract_score_count,
            session_info.rule_score_count as rule_score_count,
            session_info.rule_score as rule_score,
            session_info.rule_add_score_count as rule_add_score_count,
            session_info.rule_add_score as rule_add_score
        from (
                select `date`,
                    platform,
                    `group`,
                    seller_nick as shop_name,
                    snick,
                    count(1) AS session_count,
                    sum(
                        if(score_add > 0 or mark_score_add > 0 or rule_add_score_info > 0, 1, 0 )
                    ) as add_score_count,
                    sum(
                        if(score > 0 or mark_score > 0 or rule_score_info > 0, 1, 0 )
                    ) AS subtract_score_count,
                    sum(if(length(mark_ids) != 0, 1, 0)) as manual_qc_count,
                    sum(if(arraySum(abnormals_count) > 0, 1, 0)) AS ai_abnormal_count,
                    sum(if(length(tag_score_stats_id) > 0, 1, 0)) AS manual_abnormal_count,
                    sum(score_add) AS ai_add_score,
                    sum(mark_score_add) AS manual_add_score,
                    sum(score) as ai_subtract_score,
                    sum(mark_score) AS manual_subtract_score,
                    sum(if(score_add > 0, 1, 0)) AS ai_add_score_count,
                    sum(if(mark_score_add > 0, 1, 0)) AS manual_add_score_count,
                    sum(if(score > 0, 1, 0)) as ai_subtract_score_count,
                    sum(if(mark_score > 0, 1, 0)) AS manual_subtract_score_count,
                    sum (if(rule_score_info > 0, 1, 0)) as rule_score_count,
                    sum (rule_score_info) as rule_score,
                    sum (if(rule_add_score_info > 0, 1, 0)) as rule_add_score_count,
                    sum (rule_add_score_info) as rule_add_score
                from (
                        select dialog_info.*,
                            rule_score_info
                        from (
                                select `date`,
                                    platform,
                                    `group`,
                                    seller_nick,
                                    seller_nick as shop_name,
                                    snick,
                                    _id,
                                    score,
                                    score_add,
                                    mark_score,
                                    mark_score_add,
                                    mark_ids,
                                    abnormals_count,
                                    tag_score_stats_id
                                from dwd.xdqc_dialog_all
                                WHERE toYYYYMMDD(begin_time) = { ds_nodash }
                            ) as dialog_info
                            left join (
                                SELECT _id,
                                    sum(score) as rule_score_info
                                FROM dwd.xdqc_dialog_all array
                                    join rule_stats_score as score,
                                    rule_stats_count as count
                                WHERE toYYYYMMDD(begin_time) = { ds_nodash }
                                group by _id
                            ) as rule using(_id)
                    ) as rule_info
                    left join (
                        SELECT _id,
                            sum(score) as rule_add_score_info
                        FROM dwd.xdqc_dialog_all
                        array join
                            rule_add_stats_score as score,
                            rule_add_stats_count as count
                        WHERE toYYYYMMDD(begin_time) = { ds_nodash }
                        group by _id
                    ) rule_add using(_id)
                GROUP BY date, platform, seller_nick, group, snick
            ) as session_info
            left join (
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
                            LEFT JOIN (
                                select *
                                from ods.xinghuan_employee_snick_all
                                where day = { ds_nodash }
                                    and platform = 'tb'
                            ) AS b ON a._id = b.employee_id
                    ) AS b ON a._id = b.department_id
            ) dim_info on session_info.snick = dim_info.snick
    ) as a
    left join (
        select
            day,
            shop_name,
            snick,
            groupArray(dialog_id) as dialog_array
        from ods.xinghuan_qc_abnormal_all
        where row_number < 4
        and day = { ds_nodash }
        group by day,shop_name, snick
    ) as b 
    on a.date = b.day
    and a.shop_name = b.shop_name
    and a.snick = b.snick