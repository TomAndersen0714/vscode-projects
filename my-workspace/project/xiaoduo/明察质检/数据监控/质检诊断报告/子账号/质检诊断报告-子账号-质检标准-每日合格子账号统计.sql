-- 质检诊断报告-子账号-质检标准-每日合格子账号统计
SELECT
    day,
    uniqExact(snick) AS snick_uv,
    uniqExactIf(snick, subtract_score_dialog_cnt=0) AS qualified_snick_uv,
    snick_uv AS `质检子账号总量`,
    qualified_snick_uv AS `合格子账号总量`,
    if(snick_uv!=0, round(qualified_snick_uv/snick_uv*100, 4), 0.00) AS `子账号合格率`
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