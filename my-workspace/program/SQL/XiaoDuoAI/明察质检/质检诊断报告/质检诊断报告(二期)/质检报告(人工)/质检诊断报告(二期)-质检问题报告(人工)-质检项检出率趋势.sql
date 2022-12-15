-- 质检诊断报告(二期)-质检问题报告(人工)-质检项检出率趋势
SELECT
    day,
    tag_dialog_sum,
    dialog_sum,
    IF(dialog_sum!=0, round(tag_dialog_sum / dialog_sum * 100, 2), 0.00) AS tag_dialog_pct,
    dialog_sum AS `质检会话量`,
    tag_dialog_pct AS `检出率`
FROM (
    SELECT
        day,
        -- tag_cnt_sum 待替换 tag_manual_dialog_cnt
        SUM(tag_cnt_sum) AS tag_dialog_sum
    FROM xqc_dws.tag_stat_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
        AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
    -- 筛选指定平台
    AND platform = '{{ platform=tb }}'
    -- 筛选指定店铺
    AND seller_nick GLOBAL IN (
        SELECT DISTINCT
            seller_nick
        FROM xqc_dim.xqc_shop_all
        WHERE day = toYYYYMMDD(yesterday())
        -- 筛选指定企业
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        -- 筛选指定平台
        AND platform = '{{ platform=tb }}'
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
                -- 筛选指定企业
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                -- 筛选指定平台
                AND platform = '{{ platform=tb }}'
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
        AND platform = '{{ platform=tb }}'
        -- 下拉框-店铺主账号
        AND (
            '{{ seller_nicks }}'=''
            OR
            seller_nick IN splitByChar(',', '{{ seller_nicks }}')
        )
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
                -- 筛选指定企业
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                -- 筛选指定平台
                AND platform = '{{ platform=tb }}'
                -- 下拉框-质检标准ID
                AND qc_norm_id IN splitByChar(',', '{{ qc_norm_ids }}')
            )
        )
    )
    -- 筛选指定质检项关联分组
    AND (
        -- 下拉框-质检标准
        '{{ qc_norm_ids }}'=''
        OR
        tag_group_id GLOBAL IN (
            SELECT DISTINCT
                _id AS tag_group_id
            FROM xqc_dim.qc_norm_group_full_all
            WHERE day = toYYYYMMDD(yesterday())
            -- 筛选指定企业
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            -- 筛选指定平台
            AND platform = '{{ platform=tb }}'
        )
    )
    -- 筛选指定质检项
    AND (
        -- 下拉框-质检标准
        '{{ tag_ids }}'=''
        OR
        tag_id IN splitByChar(',','{{ tag_ids }}')
    )
    GROUP BY day
) AS tag_stat
GLOBAL RIGHT JOIN (
    SELECT
        day,
        sum(dialog_cnt) AS dialog_sum
    FROM xqc_dws.snick_stat_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
        AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
    -- 筛选指定平台
    AND platform = '{{ platform=tb }}'
    -- 筛选指定店铺
    AND seller_nick GLOBAL IN (
        SELECT DISTINCT
            seller_nick
        FROM xqc_dim.xqc_shop_all
        WHERE day = toYYYYMMDD(yesterday())
        -- 筛选指定企业
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        -- 筛选指定平台
        AND platform = '{{ platform=tb }}'
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
                AND platform = '{{ platform=tb }}'
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
        AND platform = '{{ platform=tb }}'
        -- 下拉框-店铺主账号
        AND (
            '{{ seller_nicks }}'=''
            OR
            seller_nick IN splitByChar(',', '{{ seller_nicks }}')
        )
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
                AND platform = '{{ platform=tb }}'
                -- 下拉框-质检标准ID
                AND qc_norm_id IN splitByChar(',', '{{ qc_norm_ids }}')
            )
        )
    )
    GROUP BY day
) AS dialog_stat
USING(day)
ORDER BY day ASC