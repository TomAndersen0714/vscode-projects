SELECT task.day,
    task.company_id,
    customer.name AS company_name,
    task.platform,
    task._id,
    task.task_name,
    task.create_time,
    task.update_time,
    task.date,
    task.qc_type,
    task.qc_way,
    task.account_name,
    task.target_num,
    task.mark_num,
    task.ai_num,
    task.ai_rate,
    task.human_num,
    task.human_rate,
    task.task_id,
    task.qc_norm_id
FROM xqc_ods.manual_task_record_all task
    LEFT JOIN (
        SELECT *
        FROM ods.xinghuan_company_all
        WHERE day = toYYYYMMDD(yesterday())
    ) customer ON customer._id = task.company_id