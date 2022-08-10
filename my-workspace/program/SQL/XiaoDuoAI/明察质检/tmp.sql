        SELECT
            toDate('{ds}'),
            words_info.platform,
            dim_info.company_id,
            '' AS company_name,
            dim_info.department_id,
            dim_info.department_name,
            dim_info.employee_id,
            dim_info.employee_name,
            words_info.shop_name,
            words_info.`group`,
            words_info.snick,
            words_info.source,
            words_info.word,
            words_info.words_count
        FROM (
            SELECT
                `date`,
                platform,
                seller_nick AS shop_name,
                `group`,
                snick,
                source,
                word,
                sum(count) AS words_count
            FROM dwd.xdqc_dialog_all
            ARRAY JOIN
                qc_word_word AS word,
                qc_word_source AS source,
                qc_word_count AS count
            WHERE toYYYYMMDD(begin_time) = {ds_nodash}
            AND qc_word_word != []
            GROUP BY `date`,
                    platform,
                    seller_nick,
                    `group`,
                    snick,
                    source,
                    word
        ) AS words_info
        GLOBAL LEFT JOIN (
            SELECT a.company_id AS company_id,
                b.platform,
                a._id AS department_id,
                a.name AS department_name,
                b.employee_id AS employee_id,
                b.employee_name AS employee_name,
                b.snick AS snick
            FROM (
                SELECT *
                FROM ods.xinghuan_department_all
                WHERE day = {snapshot_ds_nodash}
            ) AS a
            GLOBAL RIGHT JOIN (
                SELECT a._id AS employee_id,
                    b.platform,
                    b.department_id AS department_id,
                    a.username AS employee_name,
                    b.snick AS snick 
                FROM (
                    SELECT *
                    FROM ods.xinghuan_employee_all
                    WHERE day = {snapshot_ds_nodash}
                ) AS a
                GLOBAL RIGHT JOIN (
                    SELECT *
                    FROM ods.xinghuan_employee_snick_all
                    WHERE day = {snapshot_ds_nodash} 
                ) AS b
                ON a._id = b.employee_id
            ) AS b
            ON a._id = b.department_id
        ) AS dim_info
        ON words_info.platform = dim_info.platform
        AND words_info.snick = dim_info.snick