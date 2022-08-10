            SELECT account_employee_info.company_id AS company_id,
                account_employee_info.account_id AS account_id,
                account_employee_info.username AS username,
                account_employee_info.employee_id AS employee_id
            FROM (
                SELECT account_info.company_id AS company_id,
                    account_info.account_id AS account_id,
                    employee_info.username AS username,
                    account_info.employee_id AS employee_id
                FROM (
                    SELECT company_id,
                        _id AS account_id,
                        employee_id
                    FROM ods.xinghuan_account_all
                    WHERE day = {snapshot_ds_nodash}
                ) AS account_info
                LEFT JOIN (
                    SELECT _id AS employee_id,
                        username
                    FROM ods.xinghuan_employee_all
                    WHERE day = {snapshot_ds_nodash}
                ) AS employee_info 
                USING(employee_id)
            ) AS account_employee_info