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
            SELECT tag_info.platform,
                tag_info.seller_nick,
                tag_info.`group`,
                'manual' AS type,
                tag_info.snick,
                tag_info.tag_id AS qc_id,
                all_tag_name_info.all_tag_name AS qc_name,
                tag_info.qc_count AS qc_count
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
            ) AS tag_info
            LEFT JOIN (
                SELECT norm_tag.tag_id,
                    toString(
                        concat(
                            if(
                                norm_tag.qc_norm_name = '',
                                '未设置一级标签',
                                norm_tag.qc_norm_name
                            ),
                            '/',
                            if(
                                sub_category.name = '',
                                '未设置二级标签',
                                sub_category.name
                            ),
                            '/',
                            norm_tag.tag_name
                        )
                    ) AS all_tag_name
                FROM (
                    SELECT b._id AS qc_norm_id,
                        b.name AS qc_norm_name,
                        a._id AS tag_id,
                        a.name AS tag_name,
                        a.sub_category_id AS sub_category_id
                    FROM (
                        SELECT _id,
                            category_id,
                            sub_category_id,
                            seller_nick,
                            qc_norm_id,
                            name
                        FROM ods.xdqc_tag_all
                        WHERE day = {ds_nodash}
                    ) AS a
                    LEFT JOIN (
                        SELECT _id,
                            name
                        FROM ods.xinghuan_qc_norm_all
                        WHERE day = {ds_nodash}
                        AND status = 1
                    ) AS b 
                    ON a.qc_norm_id = b._id
                ) AS norm_tag
                LEFT JOIN (
                    SELECT _id,
                        name
                    FROM ods.xdqc_tag_sub_category_all
                    WHERE day = {ds_nodash}
                ) AS sub_category 
                ON norm_tag.sub_category_id = sub_category._id
            ) AS all_tag_name_info 
            ON tag_info.tag_id = all_tag_name_info.tag_id
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
