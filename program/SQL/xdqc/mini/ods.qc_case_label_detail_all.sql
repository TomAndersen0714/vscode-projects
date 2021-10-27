insert into ods.qc_case_label_detail_all
select toDate('{ds}'),
    'tb' as platform,
    label_info.*,
    label_detail_info.company_name,
    label_detail_info.employee_id,
    label_detail_info.employee_name,
    label_detail_info.dialog_id,
    splitByChar(':', label_detail_info.snick) [1] as shop_name,
    label_detail_info.snick
from (
        select parent_label.company_id,
            parent_label.case_label_id as parent_label_id,
            parent_label.parent_name as parent_label_name,
            label.case_label_id as label_id,
            label.name as label_name
        from (
                SELECT company_id,
                    _id as case_label_id,
                    name as parent_name
                FROM ods.xinghuan_case_label_all
                WHERE day = { ds_nodash }
                    and parent_id = ''
            ) as parent_label
            left join (
                SELECT company_id,
                    _id as case_label_id,
                    parent_id,
                    name
                FROM ods.xinghuan_case_label_all
                WHERE day = { ds_nodash }
                    and parent_id != ''
            ) as label on parent_label.company_id = label.company_id
            and parent_label.case_label_id = label.parent_id
    ) as label_info
    left join (
        select detail_info.mc_platform as platform,
            detail_info.company_id as company_id,
            '' as company_name,
            detail_info.employee_id as employee_id,
            dim_info.employee_name as employee_name,
            detail_info.dialog_id as dialog_id,
            detail_info.case_label_id as case_label_id,
            detail_info.snick as snick
        from (
                SELECT mc_platform,
                    company_id,
                    department_id,
                    employee_id,
                    dialog_id,
                    case_label_id,
                    snick
                FROM ods.xinghuan_case_detail_all
                WHERE day = { ds_nodash }
                    and platform = 'tb'
            ) as detail_info
            left join (
                SELECT a.company_id AS company_id,
                    a._id AS department_id,
                    a.name AS department_name,
                    b.employee_id AS employee_id,
                    b.employee_name AS employee_name,
                    b.snick AS snick
                FROM (
                        SELECT *
                        FROM ods.xinghuan_department_all
                        WHERE day = { ds_nodash }
                    ) AS a GLOBAL
                    LEFT JOIN (
                        SELECT a._id AS employee_id,
                            b.department_id AS department_id,
                            a.username AS employee_name,
                            b.snick AS snick
                        FROM (
                                SELECT *
                                FROM ods.xinghuan_employee_all
                                WHERE day = { ds_nodash }
                            ) AS a GLOBAL
                            LEFT JOIN (
                                SELECT *
                                FROM ods.xinghuan_employee_snick_all
                                WHERE day = { ds_nodash }
                            ) AS b ON a._id = b.employee_id
                    ) AS b ON a._id = b.department_id
            ) dim_info on detail_info.employee_id = dim_info.employee_id
            and detail_info.department_id = dim_info.department_id
            and detail_info.snick = dim_info.snick
    ) label_detail_info on label_info.label_id = label_detail_info.case_label_id
    and label_info.company_id = label_detail_info.company_id