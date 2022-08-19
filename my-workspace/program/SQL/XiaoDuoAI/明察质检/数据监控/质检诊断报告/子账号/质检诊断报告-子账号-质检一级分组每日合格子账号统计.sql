-- 质检诊断报告-子账号-质检一级分组每日合格子账号统计
SELECT
    day,
    snick_uv,
    (snick_uv - subtract_score_snick_uv) AS qualified_snick_sum,
    snick_uv AS `质检子账号总量`,
    qualified_snick_sum AS `合格子账号总量`,
    if(qualified_snick_sum!=0, round(qualified_snick_sum/snick_uv*100, 4), 0.00) AS `子账号合格率`
FROM (
    SELECT
        day,
        uniqExactIf(snick, subtract_score_dialog_cnt>0) AS subtract_score_snick_uv
    FROM (
        SELECT day, snick, subtract_score_dialog_cnt
        FROM remote('10.22.134.218:19000', xqc_dws.tag_group_stat_all)
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
        -- 下拉框-质检标准
        AND (
            '{{ qc_norm_ids }}'=''
            OR
            qc_norm_id IN splitByChar(',', '{{ qc_norm_ids }}')
        )
        -- 筛选一级质检项分组
        AND tag_group_level = 1
        -- 下拉框-一级质检项分组
        AND tag_group_id IN splitByChar(',', '{{ tag_group_ids }}')

        UNION ALL
        SELECT day, snick, subtract_score_dialog_cnt
        FROM remote('10.22.134.218:19000', xqc_dws.snick_stat_all)
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
        -- 下拉框-一级质检项分组-全部
        AND '{{ tag_group_ids }}'='all'
    )
    GROUP BY day
) AS tag_group_dialog_stat
GLOBAL FULL OUTER JOIN (
    SELECT
        day,
        uniqExact(snick) AS snick_uv
    FROM remote('10.22.134.218:19000', xqc_dws.snick_stat_all)
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
) AS dialog_stat
USING(day)
ORDER BY day ASC