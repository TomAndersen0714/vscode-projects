
                SELECT
                    company_id, department_id, COUNT(1)
                FROM (
                    SELECT DISTINCT
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
                        WHERE day = 20230818
                    ) AS qc_norm_relate_info
                    GLOBAL LEFT JOIN (
                        SELECT
                            company_id,
                            _id AS qc_norm_id,
                            name AS qc_norm_name
                        FROM ods.xinghuan_qc_norm_all
                        WHERE day = 20230818
                    ) AS qc_norm_info
                    USING(company_id, qc_norm_id)
                    -- WHERE department_id = '630f068f4f78e40c16f164db'
                )
                GROUP BY company_id, department_id