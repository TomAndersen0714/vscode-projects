-- 质检人员质检量
-- 修改需求:
/* 
质检人员不应该通过seller_nick来获取department_id, 因为质检人员不一定会同时充当客服角色, 即无法获得seller_nick, 而且
客服也不一定就会绑定seller_nick, 有些客服就是闲置状态, 并不值班
*/
insert into ods.qc_read_mark_detail_all
select toDate('{ds}') AS `day`,
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
        select platform,
            seller_nick,
            snick,
            _id as dialog_id,
            last_mark_id
        from dwd.xdqc_dialog_all
        where toYYYYMMDD(begin_time) = { ds_nodash }
            and last_mark_id != ''
    ) as dialog_info
    left join (
        SELECT account.company_id AS company_id,
            department._id AS department_id,
            department.name AS department_name,
            account.account_id AS account_id,
            account.username AS username
        FROM (
                select e_snick.company_id as company_id,
                    account_id,
                    username,
                    a_employee.employee_id as employee_id,
                    e_snick.department_id as department_id
                from (
                        SELECT account_info.account_id AS account_id,
                            employee_info.username AS username,
                            account_info.employee_id AS employee_id
                        FROM (
                                SELECT _id AS account_id,
                                    employee_id
                                FROM ods.xinghuan_account_all
                                WHERE day = { ds_nodash }
                            ) AS account_info
                            LEFT JOIN (
                                SELECT _id AS employee_id,
                                    username
                                FROM ods.xinghuan_employee_all
                                WHERE day = { ds_nodash }
                            ) AS employee_info USING(employee_id)
                    ) as a_employee
                    left join (
                        select company_id,
                            department_id,
                            employee_id
                        from ods.xinghuan_employee_snick_all
                        WHERE day = { ds_nodash }
                    ) as e_snick using(employee_id)
            ) AS account
            LEFT JOIN (
                SELECT _id,
                    company_id,
                    name
                FROM ods.xinghuan_department_all
                WHERE day = { ds_nodash }
            ) AS department ON department._id = account.department_id
    ) dim_info on dialog_info.last_mark_id = dim_info.account_id