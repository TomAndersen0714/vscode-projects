-- 质检诊断报告-会话-质检标准-每日合格会话统计
SELECT
    day,
    SUM(dialog_cnt) AS dialog_sum,
    SUM(dialog_cnt - subtract_score_dialog_cnt) AS qualified_dialog_sum,
    dialog_sum AS `质检会话总量`,
    qualified_dialog_sum AS `合格会话总量`,
    if(qualified_dialog_sum!=0, round(qualified_dialog_sum/dialog_sum*100, 4), 0.00) AS `会话合格率`
FROM xqc_dws.snick_stat_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
    AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
-- 筛选指定平台
AND platform = 'tb'
-- 筛选指定企业的店铺
AND seller_nick IN (
    SELECT DISTINCT
        seller_nick
    FROM xqc_dim.xqc_shop_all
    WHERE day = toYYYYMMDD(yesterday())
    -- 筛选指定企业
    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    -- 筛选指定平台
    AND platform = 'tb'
    -- 下拉框-店铺主账号
    AND (
        '{{ seller_nicks }}'=''
        OR
        seller_nick IN splitByChar(',', '{{ seller_nicks }}')
    )
)
-- 筛选指定质检标准对应的子账号
AND (
    '{{ qc_norm_ids }}'=''
    OR
    snick GLOBAL IN (
        -- 筛选指定子账号分组中的子账号
        SELECT snick
        FROM ods.xinghuan_employee_snick_all
        WHERE day = toYYYYMMDD(yesterday())
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        AND department_id IN (
            -- 筛选指定质检标准对应的子账号分组
            SELECT department_id
            FROM ods.xinghuan_qc_norm_relate_all
            WHERE day = toYYYYMMDD(yesterday())
            AND qc_norm_id IN splitByChar(',', '{{ qc_norm_ids }}')
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        )
    )
)
GROUP BY day
ORDER BY day ASC