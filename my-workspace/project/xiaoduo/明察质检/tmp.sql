-- 质检诊断报告(二期)-会话质检报告-会话合格率趋势
SELECT
    u_day AS day,
    dialog_sum,
    qualified_dialog_sum,
    dialog_sum AS `质检会话总量`,
    qualified_dialog_sum AS `合格会话总量`,
    if(dialog_sum!=0, round(qualified_dialog_sum/dialog_sum*100, 4), 0.00) AS `会话合格率`
FROM (
    SELECT
        u_day,
        SUM(dialog_cnt) AS dialog_sum
    FROM xqc_dws.snick_stat_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
        AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
    -- 筛选指定平台
    AND platform = 'tb'
    -- 筛选指定店铺
    AND seller_nick GLOBAL IN (
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
        -- 下拉框-质检标准
        AND (
            '{{ qc_norm_ids }}'=''
            OR
            seller_nick GLOBAL IN (
                SELECT DISTINCT
                    seller_nick
                FROM ods.xinghuan_qc_norm_relate_all
                WHERE day = toYYYYMMDD(yesterday())
                -- 筛选指定企业的质检标准
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                -- 筛选指定平台
                AND platform = 'tb'
                -- 下拉框-质检标准ID
                AND qc_norm_id IN splitByChar(',', '{{ qc_norm_ids }}')
            )
        )
    )
    -- 筛选指定子账号
    AND snick GLOBAL IN (
        SELECT snick
        FROM xqc_dim.snick_full_info_all
        WHERE day = toYYYYMMDD(yesterday())
        -- 筛选指定企业
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        -- 筛选指定平台
        AND platform = 'tb'
        -- 下拉框-子账号分组
        AND (
            '{{ department_ids }}'=''
            OR
            department_id IN splitByChar(',','{{ department_ids }}')
        )
        -- 下拉框-质检标准
        AND (
            '{{ qc_norm_ids }}'=''
            OR
            department_id GLOBAL IN (
                SELECT DISTINCT
                    department_id
                FROM ods.xinghuan_qc_norm_relate_all
                WHERE day = toYYYYMMDD(yesterday())
                -- 筛选指定企业的质检标准
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                -- 筛选指定平台
                AND platform = 'tb'
                -- 下拉框-质检标准ID
                AND qc_norm_id IN splitByChar(',', '{{ qc_norm_ids }}')
            )
        )
    )
    GROUP BY toUInt32(day) AS u_day
) AS dialog_info
GLOBAL LEFT JOIN (
    SELECT
        day,
        sum((100 - score + score_add) >= toUInt8OrZero('{{ passing_score=100 }}')) AS qualified_dialog_sum
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
        AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
    -- 筛选指定平台
    AND platform = 'tb'
    -- 筛选指定店铺
    AND seller_nick GLOBAL IN (
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
        -- 下拉框-质检标准
        AND (
            '{{ qc_norm_ids }}'=''
            OR
            seller_nick GLOBAL IN (
                SELECT DISTINCT
                    seller_nick
                FROM ods.xinghuan_qc_norm_relate_all
                WHERE day = toYYYYMMDD(yesterday())
                -- 筛选指定企业的质检标准
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                -- 筛选指定平台
                AND platform = 'tb'
                -- 下拉框-质检标准ID
                AND qc_norm_id IN splitByChar(',', '{{ qc_norm_ids }}')
            )
        )
    )
    -- 筛选指定子账号
    AND snick GLOBAL IN (
        SELECT snick
        FROM xqc_dim.snick_full_info_all
        WHERE day = toYYYYMMDD(yesterday())
        -- 筛选指定企业
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        -- 筛选指定平台
        AND platform = 'tb'
        -- 下拉框-子账号分组
        AND (
            '{{ department_ids }}'=''
            OR
            department_id IN splitByChar(',','{{ department_ids }}')
        )
        -- 下拉框-质检标准
        AND (
            '{{ qc_norm_ids }}'=''
            OR
            department_id GLOBAL IN (
                SELECT DISTINCT
                    department_id
                FROM ods.xinghuan_qc_norm_relate_all
                WHERE day = toYYYYMMDD(yesterday())
                -- 筛选指定企业的质检标准
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                -- 筛选指定平台
                AND platform = 'tb'
                -- 下拉框-质检标准ID
                AND qc_norm_id IN splitByChar(',', '{{ qc_norm_ids }}')
            )
        )
    )
    GROUP BY toYYYYMMDD(begin_time) AS day
) AS qualified_dialog_info
ON dialog_info.u_day = qualified_dialog_info.day
ORDER BY day ASC