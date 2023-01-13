-- 
insert into ods.xinghuan_qc_task_detail_all
select toDate('{ds}') as `date`,
    platform,
    task_id,
    task_table.company_id,
    snick_table.department_id,
    snick_table.department_name,
    task_table.qc_norm_id,
    task_table.qc_norm_name,
    account_id,
    username,
    dialog_id,
    is_mark,
    date,
    seller_nick,
    task_table.snick,
    snick_table.employee_name,
    snick_table.superior_name,
    cnick,
    begin_time,
    end_time,
    order_id,
    mark,
    mark_score,
    mark_score_add,
    score,
    score_add,
    abnormals_type,
    abnormals_count,
    excellents_type,
    excellents_count,
    s_emotion_type,
    s_emotion_count,
    c_emotion_type,
    c_emotion_count,
    tag_score_stats_id,
    tag_score_add_stats_id
from (
        select platform,
            qc_task_id as task_id,
            company_id,
            department_id,
            department_name,
            qc_norm_id,
            qc_norm_name,
            account_id,
            username,
            dialog_id,
            is_mark,
            date,
            seller_nick,
            snick,
            cnick,
            begin_time,
            end_time,
            order_id,
            mark,
            mark_score,
            mark_score_add,
            score,
            score_add,
            abnormals_type,
            abnormals_count,
            excellents_type,
            excellents_count,
            s_emotion_type,
            s_emotion_count,
            c_emotion_type,
            c_emotion_count,
            tag_score_stats_id,
            tag_score_add_stats_id
        from (
                select _id,
                    seller_nick,
                    snick,
                    cnick,
                    begin_time,
                    end_time,
                    order_info_id [1] as order_id,
                    mark,
                    mark_score,
                    mark_score_add,
                    score,
                    score_add,
                    abnormals_type,
                    abnormals_count,
                    excellents_type,
                    excellents_count,
                    s_emotion_type,
                    s_emotion_count,
                    c_emotion_type,
                    c_emotion_count,
                    tag_score_stats_id,
                    tag_score_add_stats_id
                from dwd.xdqc_dialog_all
                where toYYYYMMDD(begin_time) between { yesterday_ds_nodash } and { ds_nodash }
                    and platform = 'tb'
            ) as dialog_all
            left join (
                select platform,
                    qc_task_id,
                    company_id,
                    department_id,
                    department_name,
                    qc_norm_id,
                    qc_norm_name,
                    account_id,
                    username,
                    dialog_id,
                    is_mark,
                    date
                from (
                        select platform,
                            _id,
                            company_id,
                            department_id,
                            department_name,
                            qc_norm_id,
                            qc_norm_name,
                            account_id,
                            b.username
                        from (
                                select platform,
                                    _id,
                                    company_id,
                                    department_id,
                                    department_name,
                                    qc_norm_id,
                                    b.name as qc_norm_name,
                                    account_id
                                from (
                                        select platform,
                                            _id,
                                            company_id,
                                            department_id,
                                            b.name as department_name,
                                            qc_norm_id,
                                            account_id
                                        from (
                                                select platform,
                                                    _id,
                                                    company_id,
                                                    department_id,
                                                    qc_norm_id,
                                                    account_id
                                                from ods.xinghuan_qc_task_all
                                                where day = { ds_nodash }
                                                    and platform = 'tb'
                                            ) as a
                                            left join(
                                                select _id,
                                                    name
                                                from ods.xinghuan_department_all
                                                where day = { ds_nodash }
                                            ) as b on a.department_id = b._id
                                    ) as a
                                    left join (
                                        select _id,
                                            name
                                        from ods.xinghuan_qc_norm_all
                                        where day = { ds_nodash }
                                    ) as b on a.qc_norm_id = b._id
                            ) as a
                            left join (
                                select a.account_id as account_id,
                                    a.company_id as company_id,
                                    a.employee_id as employee_id,
                                    e.employee_name as username
                                from (
                                        select _id as account_id,
                                            employee_id,
                                            company_id,
                                            username
                                        from ods.xinghuan_account_all
                                        where day = { ds_nodash }
                                    ) as a
                                    left join (
                                        select company_id,
                                            department_id,
                                            _id as employee_id,
                                            superior_id,
                                            superior_name,
                                            username as employee_name
                                        from ods.xinghuan_employee_all
                                        where day = { ds_nodash }
                                    ) as e on a.employee_id = e.employee_id
                                    and a.company_id = e.company_id
                            ) as b on a.account_id = b.account_id
                    ) as task_info
                    left join (
                        select qc_task_id,
                            dialog_id,
                            is_mark,
                            toYYYYMMDD(toDate(toInt32(date))) as date
                        from ods.xinghuan_qc_task_instance_all
                        where day = { ds_nodash }
                            and date = { ds_nodash }
                    ) as instanc_info on task_info._id = instanc_info.qc_task_id
            ) as task_info_all on task_info_all.dialog_id = dialog_all._id
    ) as task_table
    left join (
        select department_info.company_id as company_id,
            department_info.department_id as department_id,
            department_info.department_name as department_name,
            department_info.employee_id as employee_id,
            department_info.superior_id as superior_id,
            department_info.superior_name as superior_name,
            department_info.employee_name as employee_name,
            department_info.snick as snick,
            account.account_id as account_id
        from (
                select a.company_id,
                    a.department_id,
                    b.department_name,
                    a.employee_id,
                    a.superior_id,
                    a.superior_name,
                    a.employee_name,
                    a.snick
                from (
                        select a.company_id,
                            b.department_id as department_id,
                            a.employee_id,
                            a.superior_id,
                            a.superior_name,
                            a.employee_name,
                            b.snick
                        from (
                                select company_id,
                                    department_id,
                                    _id as employee_id,
                                    superior_id,
                                    superior_name,
                                    username as employee_name
                                from ods.xinghuan_employee_all
                                where day = { ds_nodash }
                            ) as a
                            left join (
                                select platform,
                                    company_id,
                                    employee_id,
                                    snick,
                                    department_id
                                from ods.xinghuan_employee_snick_all
                                where day = { ds_nodash }
                                    and platform = 'tb'
                            ) as b on a.employee_id = b.employee_id
                            and a.company_id = b.company_id
                    ) as a
                    left join (
                        select _id,
                            name as department_name
                        from ods.xinghuan_department_all
                        where day = { ds_nodash }
                    ) as b on a.department_id = b._id
            ) as department_info
            left join (
                select a.account_id as account_id,
                    a.company_id as company_id,
                    a.employee_id as employee_id,
                    e.employee_name as username
                from (
                        select _id as account_id,
                            employee_id,
                            company_id,
                            username
                        from ods.xinghuan_account_all
                        where day = { ds_nodash }
                    ) as a
                    left join (
                        select company_id,
                            department_id,
                            _id as employee_id,
                            superior_id,
                            superior_name,
                            username as employee_name
                        from ods.xinghuan_employee_all
                        where day = { ds_nodash }
                    ) as e on a.employee_id = e.employee_id
                    and a.company_id = e.company_id
            ) as account on department_info.company_id = account.company_id
            and department_info.employee_id = account.employee_id
    ) as snick_table on task_table.company_id = snick_table.company_id
    and task_table.snick = snick_table.snick