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
    select department.company_id as company_id,
        department._id as department_id,
        department.name as department_name,
        account.account_id as account_id,
        account.username as username
    from (
        select _id,
            company_id,
            name
        from ods.xinghuan_department_all
        where day = { ds_nodash }
    ) as department
    left join (
        select
            account_info.account_id as account_id,
            employee_info.username as username,
            employee_info.department_id as department_id
        from (
            select _id as account_id,
                employee_id
            from ods.xinghuan_account_all
            where day = { ds_nodash }
        ) as account_info
        left join (
            select _id as employee_id,
                department_id,
                username
            from ods.xinghuan_employee_all
            where day = { ds_nodash }
        ) as employee_info using(employee_id)
    ) as account
    on department._id = account.department_id
) dim_info 
on dialog_info.last_mark_id = dim_info.account_id