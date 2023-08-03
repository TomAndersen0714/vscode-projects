SELECT
    platform AS `平台`,
    seller_nick AS `店铺`, department_name AS `子账号分组`, snick AS `客服子账号`,
    employee_name AS `客服姓名`, superior_name AS `客服上级姓名`,
    qc_norm_name AS `质检标准`,
    tag_name AS `质检项名称`,
    tag_cnt_sum AS `质检项触发次数`,
    tag_score_sum AS `质检项分值`,
    tag_dialog_cnt_sum AS `质检项打标会话量`,
    round(tag_score_sum/tag_dialog_cnt_sum,2) AS tag_score_sum_avg_score AS `质检项平均分`
FROM (
    SELECT
        platform, seller_nick, snick, 
        qc_norm_name, tag_type, tag_id, tag_name,
        tag_cnt_sum, tag_score_sum, tag_dialog_cnt_sum
    FROM (
        SELECT
            platform, seller_nick, snick, 
            tag_type, tag_id, tag_name,
            sum(tag_cnt_sum) AS tag_cnt_sum,
            sum(tag_score_sum) AS tag_score_sum,
            sum(tag_dialog_cnt) AS tag_dialog_cnt_sum
        FROM xqc_dws.tag_stat_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start }}')) AND toYYYYMMDD(toDate('{{ day.end }}'))
        AND platform = '{{platform}}'
        AND seller_nick GLOBAL IN (
            -- 查询对应企业-平台的店铺
            SELECT DISTINCT seller_nick
            FROM xqc_dim.shop_latest_all
            WHERE platform = '{{platform}}'
            AND company_id = '{{ company_id }}'
        )
        GROUP BY platform, seller_nick, snick, tag_type, tag_id, tag_name
    )
    LEFT JOIN (
        SELECT
            tag_id, qc_norm_name
        FROM (
            SELECT
                _id AS tag_id, name AS tag_name, qc_norm_id
            FROM xqc_dim.qc_rule_all
            WHERE day = toYYYYMMDD(yesterday())
            AND status = 1
            AND qc_norm_id GLOBAL IN (
                SELECT
                    _id
                FROM ods.xinghuan_qc_norm_all
                WHERE day = toYYYYMMDD(yesterday())
                AND platform = '{{platform}}'
                AND company_id = '{{ company_id }}'
                AND status = 1
            )
        ) AS qc_rule_info
        GLOBAL LEFT JOIN (
            SELECT
                _id AS qc_norm_id, name AS qc_norm_name
            FROM ods.xinghuan_qc_norm_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = '{{platform}}'
            AND company_id = '{{ company_id }}'
            AND status = 1
        ) AS qc_norm_info
        USING(qc_norm_id)
    )
    USING(tag_id)
) AS tag_stat
GLOBAL LEFT JOIN (
    -- 关联子账号分组/子账号员工信息
    SELECT
        platform, snick, employee_name, superior_name, department_id, department_name
    FROM xqc_dim.snick_full_info_all
    WHERE day = toYYYYMMDD(yesterday())
    AND platform = '{{platform}}'
    AND company_id = '{{ company_id }}'
) AS dim_snick_department
USING(platform, snick)