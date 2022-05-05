        INSERT INTO ods.qc_question_detail_all
        SELECT toDate('{ds}'),
            manual_qc_info.platform AS platform,
            dim_info.company_id,
            '' AS company_name,
            dim_info.department_id,
            dim_info.department_name,
            dim_info.employee_id,
            dim_info.employee_name,
            manual_qc_info.seller_nick AS shop_name,
            manual_qc_info.`group`,
            manual_qc_info.`type`,
            manual_qc_info.qc_id,
            manual_qc_info.qc_name,
            manual_qc_info.qc_count
        FROM (
            SELECT tag_stat.platform,
                tag_stat.seller_nick,
                tag_stat.`group`,
                'manual' AS type,
                tag_stat.snick,
                tag_stat.tag_id AS qc_id,
                tag_full_name_info.tag_full_name AS qc_name,
                tag_stat.qc_count AS qc_count
            FROM (
                SELECT tag_id,
                    platform,
                    seller_nick,
                    `group`,
                    snick,
                    count(1) AS qc_count
                FROM ods.xinghuan_dialog_tag_score_all
                WHERE day = {ds_nodash}
                AND cal_op = 0
                GROUP BY tag_id, platform, seller_nick, `group`, snick
            ) AS tag_stat
            LEFT JOIN (
                -- 人工质检标签: 质检标准/质检项分组名/质检项名
                SELECT
                    tag_id,
                    CONCAT(
                        if(qc_norm_name = '', '未知标准', qc_norm_name),
                        '/',
                        if(tag_group_full_name = '', '未知分组', tag_group_full_name),
                        '/',
                        if(tag_name = '', '未知质检项', tag_name)
                    ) AS tag_full_name
                FROM (
                    SELECT
                        tag_id,
                        tag_name,
                        tag_group_id,
                        qc_norm_id,
                        qc_norm_name
                    FROM (
                        -- 查询人工质检项
                        SELECT
                            _id AS tag_id,
                            name AS tag_name,
                            qc_norm_group_id AS tag_group_id,
                            qc_norm_id
                        FROM xqc_dim.qc_rule_all
                        WHERE day = toYYYYMMDD(yesterday)
                        AND rule_category = 2
                    ) AS tag_info
                    GLOBAL LEFT JOIN (
                        -- 关联质检标准
                        SELECT
                            _id AS qc_norm_id,
                            name AS qc_norm_name
                        FROM ods.xinghuan_qc_norm_all
                        WHERE day = toYYYYMMDD(yesterday)
                    ) AS qc_norm_info
                    USING(qc_norm_id)
                ) AS tag_qc_norm_info
                GLOBAL LEFT JOIN (
                    -- 关联质检项分组
                    SELECT
                        _id AS tag_group_id,
                        full_name AS tag_group_full_name
                    FROM xqc_dim.qc_norm_group_path_all
                    WHERE day = toYYYYMMDD(yesterday)
                ) AS tag_group_info
                USING(tag_group_id)
            ) AS tag_full_name_info
            USING(tag_id)
        ) AS manual_qc_info
        LEFT JOIN (
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
                WHERE day = {ds_nodash}
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
                    WHERE day = {ds_nodash}
                ) AS a 
                GLOBAL RIGHT JOIN (
                    SELECT *
                    FROM ods.xinghuan_employee_snick_all
                    WHERE day = {ds_nodash}
                ) AS b 
                ON a._id = b.employee_id
            ) AS b 
            ON a._id = b.department_id
        ) AS dim_info 
        ON manual_qc_info.platform = dim_info.platform
        AND manual_qc_info.snick = dim_info.snick
