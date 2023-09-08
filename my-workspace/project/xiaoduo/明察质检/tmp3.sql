INSERT INTO ods.xinghuan_dialog_tag_score_all
            SELECT
                seller_nick, platform,
                company_id, shop_id, group, snick,
                employee_id, employee_name, department_id, department_name,
                mark_account_id, account_info.account_name AS mark_account_name,
                qc_norm_id, qc_norm_name, qc_norm_group_id, qc_norm_group_name, qc_norm_group_full_name,
                dialog_id, cnick, tag_id, tag_name, tag_score, cal_op, day
            FROM (
                SELECT
                    seller_nick, platform,
                    company_id, shop_id, group, snick,
                    employee_id, employee_name, department_id, department_name,
                    mark_account_id,
                    qc_norm_id, qc_norm_name, qc_norm_group_id, qc_norm_group_name, qc_norm_group_full_name,
                    dialog_id, cnick, tag_id, tag_name, tag_score, cal_op, day
                FROM (
                    SELECT
                        day,
                        company_id, shop_id,
                        platform, seller_nick, group, snick,
                        employee_id, employee_name, department_id, department_name,
                        mark_account_id,
                        dialog_id, cnick, tag_id, tag_score, cal_op
                    FROM (
                        SELECT
                            toYYYYMMDD(begin_time) AS day,
                            platform,
                            seller_nick,
                            group,
                            snick,
                            _id AS dialog_id,
                            cnick,
                            tag_id,
                            tag_score,
                            1 AS cal_op,
                            last_mark_id AS mark_account_id
                        FROM dwd.xdqc_dialog_all
                        ARRAY JOIN
                            tag_score_add_stats_id AS tag_id,
                            tag_score_add_stats_score AS tag_score
                        WHERE toYYYYMMDD(begin_time) = {ds_nodash}
                        AND length(tag_score_add_stats_id) > 0
                    ) AS tag_dialog_info
                    GLOBAL LEFT JOIN (
                        SELECT
                            company_id, shop_id, platform, snick,
                            employee_id, employee_name, department_id, department_name
                        FROM xqc_dim.snick_full_info_all
                        WHERE day = {snapshot_ds_nodash}
                    ) AS snick_info
                    USING(platform, snick)
                ) AS tag_record
                LEFT JOIN (
                    -- 关联人工质检项
                    SELECT
                        company_id,
                        platform,
                        _id AS tag_id,
                        name AS tag_name,
                        qc_norm_id,
                        qc_norm_name,
                        qc_norm_group_id,
                        qc_norm_group_name,
                        qc_norm_group_full_name
                    FROM xqc_dim.qc_rule_full_info_latest_all
                    WHERE rule_category = 2
                ) AS tag_info 
                USING(tag_id)
            ) AS tag_detial
            GLOBAL LEFT JOIN (
                SELECT
                    _id AS account_id,
                    username AS account_name
                FROM ods.xinghuan_account_all
                WHERE day = {snapshot_ds_nodash}
            ) AS account_info
            ON tag_detial.mark_account_id = account_info.account_id