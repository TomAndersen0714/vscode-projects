SELECT
    company_id, company_name, company_short_name, platform,
    shop_id, shop_name, seller_nick,
    department_id, department_name,
    qc_norm_id, qc_norm_name,
    snick, employee_id, employee_name, superior_id, superior_name,
    day
FROM (
    SELECT
        company_id, company_name, company_short_name, platform,
        shop_id, shop_name, seller_nick,
        department_id, department_name,
        snick, employee_id, employee_name, superior_id, superior_name,
        day
    FROM (
        SELECT
            company_id, platform,
            shop_id, shop_name, seller_nick,
            department_id, department_name,
            snick, employee_id, employee_name, superior_id, superior_name,
            day
        FROM (
            SELECT
                company_id, platform,
                shop_id,
                department_id, department_name,
                snick, employee_id, employee_name, superior_id, superior_name,
                day
            FROM (
                SELECT
                    *
                FROM (
                    SELECT
                        company_id, platform,
                        mp_shop_id AS shop_id,
                        department_id,
                        snick, employee_id,
                        day
                    FROM {snick_info_tbl}
                    WHERE day = {snapshot_ds_nodash}
                ) AS snick_info
                GLOBAL LEFT JOIN (
                    SELECT DISTINCT
                        company_id,
                        _id AS employee_id,
                        username AS employee_name,
                        superior_id,
                        superior_name
                    FROM {employee_info_tbl}
                    WHERE day = {snapshot_ds_nodash}
                ) AS employee_info
                USING(company_id, employee_id)
            ) AS snick_employee_info
            GLOBAL LEFT JOIN (
                SELECT DISTINCT
                    company_id,
                    _id AS department_id,
                    full_name AS department_name
                FROM {department_info_tbl}
                WHERE day = {snapshot_ds_nodash}
            ) AS department_info
            USING (company_id, department_id)
        ) AS snick_employee_department_info
        GLOBAL LEFT JOIN (
            SELECT DISTINCT
                company_id,
                shop_id,
                seller_nick,
                plat_shop_name AS shop_name
            FROM {shop_info_tbl}
            WHERE day = {snapshot_ds_nodash}
        ) AS shop_info
        USING(company_id, shop_id)
    ) AS snick_employee_department_shop_info
    GLOBAL LEFT JOIN (
        SELECT DISTINCT
            _id AS company_id,
            name AS company_name,
            shot_name AS company_short_name
        FROM {company_info_tbl}
        WHERE day = {snapshot_ds_nodash}
    ) AS company_info
    USING(company_id)
) AS snick_info
GLOBAL LEFT JOIN (
    SELECT
        company_id,
        qc_norm_id,
        qc_norm_name,
        department_id
    FROM (
        SELECT DISTINCT
            company_id,
            department_id,
            qc_norm_id
        FROM ods.xinghuan_qc_norm_relate_all
        WHERE day = {snapshot_ds_nodash}
    ) AS qc_norm_relate_info
    GLOBAL LEFT JOIN (
        SELECT
            company_id,
            _id AS qc_norm_id,
            name AS qc_norm_name
        FROM ods.xinghuan_qc_norm_all
        WHERE day = {snapshot_ds_nodash}
    ) AS qc_norm_info
    USING(company_id, qc_norm_id)
) AS qc_norm_department_info
USING(company_id, department_id)