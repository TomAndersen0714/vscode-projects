insert into ods.qc_read_mark_detail_all
select 
    toDate('{ds}') AS `day`,
    dialog_info.platform AS platform,
    dim_info.company_id AS company_id,
    '' AS company_name,
    dim_info.department_id AS department_id,
    dim_info.department_name AS department_name,
    dialog_info.last_mark_id AS account_id,
    dim_info.username AS username,
    dialog_info.seller_nick AS seller_nick,
    dialog_info.dialog_id AS dialog_id
from (
        select 
            platform,
            seller_nick,
            snick,
            _id AS dialog_id,
            last_mark_id
        from dwd.xdqc_dialog_all
        where toYYYYMMDD(begin_time) = { ds_nodash }
            and last_mark_id != ''
    ) AS dialog_info
    left join (
        SELECT 
            account.company_id AS company_id,
            department._id AS department_id,
            department.name AS department_name,
            account.account_id AS account_id,
            account.username AS username
        FROM (
                select 
                    a_employee.company_id AS company_id,
                    a_employee.account_id AS account_id,
                    a_employee.username AS username,
                    a_employee.employee_id AS employee_id,
                    e_snick.department_id AS department_id
                from (
                    SELECT 
                        account_info.company_id AS company_id,
                        account_info.account_id AS account_id,
                        employee_info.username AS username,
                        account_info.employee_id AS employee_id
                    FROM (
                        SELECT 
                            company_id,
                            _id AS account_id,
                            employee_id
                        FROM ods.xinghuan_account_all
                        WHERE day = { ds_nodash }
                    ) AS account_info
                    LEFT JOIN (
                        SELECT 
                            _id AS employee_id,
                            username
                        FROM ods.xinghuan_employee_all
                        WHERE day = { ds_nodash }
                    ) AS employee_info
                    USING(employee_id)
                ) AS a_employee
                left join (
                    select 
                        company_id,
                        department_id,
                        employee_id
                    from ods.xinghuan_employee_snick_all
                    WHERE day = { ds_nodash }
                ) AS e_snick 
                using(employee_id)
            ) AS account
            LEFT JOIN (
                SELECT 
                    _id,
                    company_id,
                    name
                FROM ods.xinghuan_department_all
                WHERE day = { ds_nodash }
            ) AS department 
            ON department._id = account.department_id
    ) dim_info 
    on dialog_info.last_mark_id = dim_info.account_id