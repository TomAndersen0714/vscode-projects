insert into ods.qc_read_mark_detail_all
select
    toDate('{{ ds }}') AS `day`,
    dialog_info.platform as platform,
    dim_info.company_id as company_id,
    '' as company_name,
    dim_info.department_id as department_id,
    dim_info.department_name as department_name,
    dialog_info.last_mark_id as account_id,
    dim_info.username as username,
    dialog_info.seller_nick as seller_nick,
    dialog_info.dialog_id as dialog_id
from (
        select
            platform,
            seller_nick,
            snick,
            _id as dialog_id,
            last_mark_id
        from dwd.xdqc_dialog_all
        where toYYYYMMDD(begin_time) = toYYYYMMDD(toDate('{{ ds }}')) and last_mark_id != ''
    ) as dialog_info
    left join (
        SELECT
            account.company_id AS company_id,
            department._id AS department_id,
            department.name AS department_name,
            account.account_id AS account_id,
            account.username AS username
        FROM (
                select
                    e_snick.company_id as company_id,
                    account_id,
                    username,
                    a_employee.employee_id as employee_id,
                    e_snick.department_id as department_id
                from (
                        SELECT
                            account_info.account_id AS account_id,
                            employee_info.username AS username,
                            account_info.employee_id AS employee_id
                        FROM (
                            SELECT
                                _id AS account_id,
                                employee_id
                            FROM ods.xinghuan_account_all
                            WHERE day = toYYYYMMDD(toDate('{{ ds }}'))
                        ) AS account_info
                        LEFT JOIN (
                            SELECT
                                _id AS employee_id,
                                username
                            FROM ods.xinghuan_employee_all
                            WHERE day = toYYYYMMDD(toDate('{{ ds }}'))
                        ) AS employee_info 
                        USING(employee_id)
                    ) as a_employee
                    left join (
                        select
                            company_id,
                            department_id,
                            employee_id
                        from ods.xinghuan_employee_snick_all
                        WHERE day = toYYYYMMDD(toDate('{{ ds }}'))
                    ) as e_snick 
                    using(employee_id)
                    -- PS: 未绑定员工的子账号, 无法关联到 employee和account, 但是所有员工信息都获取到了
            ) AS account
            LEFT JOIN (
                SELECT
                    _id,
                    company_id,
                    name
                FROM ods.xinghuan_department_all
                WHERE day = toYYYYMMDD(toDate('{{ ds }}'))
            ) AS department 
            ON department._id = account.department_id
    ) dim_info 
    on dialog_info.last_mark_id = dim_info.account_id