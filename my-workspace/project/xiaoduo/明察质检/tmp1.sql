SELECT
    day AS `日期`,
    platform AS `平台`,
    shop_name AS `店铺名`,
    seller_nick AS `店铺主账号`,
    department_name AS `子账号分组`,
    snick AS `子账号`,
    employee_name AS `客服姓名`,
    superior_name AS `客服上级姓名`,
    dialog_cnt AS `会话总量`,
    dialog_avg_score AS `会话平均分`
FROM (
    SELECT
        day,
        platform,
        seller_nick,
        snick,
        dialog_cnt,
        round((dialog_cnt*100 + add_score_sum- subtract_score_sum)/dialog_cnt,2) AS dialog_avg_score
    FROM xqc_dws.snick_stat_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start }}')) AND toYYYYMMDD(toDate('{{ day.end }}'))
    AND platform = '{{platform}}'
    AND seller_nick GLOBAL IN (
        -- 查询对应企业-平台的店铺
        SELECT DISTINCT seller_nick
        FROM xqc_dim.shop_latest_all
        WHERE platform = '{{platform}}'
        AND company_id = '{{ company_id }}'
    )
) AS snick_stat
GLOBAL LEFT JOIN (
    -- 关联子账号分组/子账号员工信息
    SELECT
        platform, shop_name, snick, employee_name, superior_name, department_id, department_name
    FROM xqc_dim.snick_full_info_all
    WHERE day = toYYYYMMDD(yesterday())
    AND platform = '{{platform}}'
    AND company_id = '{{ company_id }}'
) AS snick_info
USING(platform, snick)