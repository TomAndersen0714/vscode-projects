SELECT a.company_id AS company_id,
    b.platform,
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
    RIGHT JOIN (
        SELECT a._id AS employee_id,
            b.platform,
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
            ) AS b ON a._id = b.employee_id
    ) AS b ON a._id = b.department_id