SELECT
    platform AS `平台`,
    seller_nick AS `店铺`, department_name AS `子账号分组`, snick AS `客服子账号`,
    employee_name AS `客服姓名`, superior_name AS `客服上级姓名`,
    dialog_cnt_sum AS `子账号会话总量`,
    dialog_avg_score AS `子账号会话平均分`,
    qc_norm_name AS `质检标准`,
    qc_norm_group_full_name AS `质检项分组全名`,
    splitByChar('/', qc_norm_group_full_name)[1] AS `质检项一级分组`,
    splitByChar('/', qc_norm_group_full_name)[2] AS `质检项二级分组`,
    splitByChar('/', qc_norm_group_full_name)[3] AS `质检项三级分组`,
    tag_name AS `质检项名称`,
    tag_cnt_sum AS `质检项触发次数`,
    tag_score_sum AS `质检项分值`,
    tag_dialog_cnt_sum AS `质检项打标会话量`,
    round(IF(tag_dialog_cnt_sum!=0, tag_score_sum*1.0/tag_dialog_cnt_sum, 0.0), 2) AS `质检项平均分1`,
    round(IF(dialog_cnt_sum!=0, tag_score_sum*1.0/dialog_cnt_sum, 0.0),2) AS `质检项平均分2`
FROM (
    SELECT
        platform, seller_nick, snick,
        dialog_cnt_sum, dialog_avg_score,
        qc_norm_id, qc_norm_name, 
        qc_norm_group_id, qc_norm_group_full_name,
        tag_id, tag_name,
        tag_cnt_sum, tag_score_sum, tag_dialog_cnt_sum
    FROM (
        SELECT
            platform, seller_nick, snick,
            sum(dialog_cnt) AS dialog_cnt_sum,
            sum(add_score_sum) AS add_score_sum_sum,
            sum(subtract_score_sum) AS subtract_score_sum_sum,
            round((dialog_cnt_sum*100 + add_score_sum_sum- subtract_score_sum_sum)/dialog_cnt_sum,2) AS dialog_avg_score
        FROM xqc_dws.snick_stat_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=yesterday }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
        AND platform = 'tb'
        AND seller_nick GLOBAL IN (
            -- 查询对应企业-平台的店铺
            SELECT DISTINCT seller_nick
            FROM xqc_dim.shop_latest_all
            WHERE platform = 'tb'
            AND company_id = '649ab10105379c4f6cf0ac05'
        )
        GROUP BY platform, seller_nick, snick
    ) AS snick_stat
    GLOBAL LEFT JOIN (
        SELECT
            platform, seller_nick, snick, 
            qc_norm_id, qc_norm_name, 
            qc_norm_group_id, qc_norm_group_full_name,
            tag_id, tag_name,
            tag_cnt_sum, tag_score_sum, tag_dialog_cnt_sum
        FROM (
            SELECT
                platform, seller_nick, snick, 
                tag_id, tag_name,
                sum(tag_cnt_sum) AS tag_cnt_sum,
                sum(tag_score_sum) AS tag_score_sum,
                sum(tag_dialog_cnt) AS tag_dialog_cnt_sum
            FROM xqc_dws.tag_stat_all
            WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=yesterday }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
            AND platform = 'tb'
            AND seller_nick GLOBAL IN (
                -- 查询对应企业-平台的店铺
                SELECT DISTINCT seller_nick
                FROM xqc_dim.shop_latest_all
                WHERE platform = 'tb'
                AND company_id = '649ab10105379c4f6cf0ac05'
            )
            GROUP BY platform, seller_nick, snick, tag_id, tag_name
        )
        LEFT JOIN (
            SELECT
                qc_norm_id, qc_norm_name, 
                qc_norm_group_id, qc_norm_group_full_name,
                tag_id, tag_name
            FROM (
                SELECT
                    qc_norm_id, qc_norm_name, qc_norm_group_id, tag_id, tag_name
                FROM (
                    SELECT
                        _id AS tag_id,
                        name AS tag_name,
                        qc_norm_id,
                        qc_norm_group_id
                    FROM xqc_dim.qc_rule_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND status = 1
                    AND qc_norm_id GLOBAL IN (
                        SELECT
                            _id
                        FROM ods.xinghuan_qc_norm_all
                        WHERE day = toYYYYMMDD(yesterday())
                        AND platform = 'tb'
                        AND company_id = '649ab10105379c4f6cf0ac05'
                        AND status = 1
                    )
                ) AS qc_rule_info
                GLOBAL LEFT JOIN (
                    SELECT
                        _id AS qc_norm_id,
                        name AS qc_norm_name
                    FROM ods.xinghuan_qc_norm_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND platform = 'tb'
                    AND company_id = '649ab10105379c4f6cf0ac05'
                    AND status = 1
                ) AS qc_norm_info
                USING(qc_norm_id)
            ) AS qc_rule_info
            GLOBAL LEFT JOIN (
                SELECT
                    _id AS qc_norm_group_id,
                    full_name AS qc_norm_group_full_name
                FROM xqc_dim.qc_norm_group_full_all
                WHERE day = toYYYYMMDD(yesterday())
                AND qc_norm_id GLOBAL IN (
                    SELECT _id AS qc_norm_id
                    FROM ods.xinghuan_qc_norm_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND platform = '{{ platform }}'
                    AND company_id = '649ab10105379c4f6cf0ac05'
                )
            ) AS qc_norm_group_info
            USING(qc_norm_group_id)
        )
        USING(tag_id)
    ) AS tag_stat
    USING(platform, seller_nick, snick)
) AS tag_stat
GLOBAL LEFT JOIN (
    -- 关联子账号分组/子账号员工信息
    SELECT
        platform, snick, employee_name, superior_name, department_id, department_name
    FROM xqc_dim.snick_full_info_all
    WHERE day = toYYYYMMDD(yesterday())
    AND platform = 'tb'
    AND company_id = '649ab10105379c4f6cf0ac05'
) AS dim_snick_department
USING(platform, snick)