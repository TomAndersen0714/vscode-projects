-- 质检诊断报告-子账号-质检标准-合格子账号统计
SELECT
    cur_period.snick_sum AS `质检子账号总量`,
    cur_period.qualified_snick_sum AS `合格子账号总量`,
    pre_period.snick_sum AS `上期质检子账号总量`,
    pre_period.qualified_snick_sum AS `上期合格子账号总量`,
    cur_period.snick_sum - pre_period.snick_sum AS dialog_cnt_diff,
    cur_period.qualified_snick_sum - pre_period.qualified_snick_sum AS qualified_dialog_cnt_diff,
    CONCAT(
        toString(
            if(dialog_cnt_diff!=0, round(pre_period.snick_sum/dialog_cnt_diff*100,2), 0.00)
        ),'%'
    ) AS `环比1`,
    CONCAT(
        toString(
            if(qualified_dialog_cnt_diff!=0, round(pre_period.qualified_snick_sum/qualified_dialog_cnt_diff*100,2), 0.00)
        ),'%'
    ) AS `环比2`,
    if(cur_period.qualified_snick_sum!=0, round(cur_period.qualified_snick_sum/cur_period.snick_sum, 4), 0.00) AS `子账号合格率`
FROM (
    SELECT
        uniqExact(snick) AS snick_sum,
        uniqExactIf(snick, subtract_score_dialog_cnt=0) AS qualified_snick_sum
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
) AS cur_period
GLOBAL CROSS JOIN (
    SELECT
        uniqExact(snick) AS snick_sum,
        uniqExactIf(snick, subtract_score_dialog_cnt=0) AS qualified_snick_sum
    FROM remote('10.22.134.218:19000', xqc_dws.snick_stat_all)
    WHERE day BETWEEN toYYYYMMDD(
            toDate('{{ day.start=week_ago }}') - (toDate('{{ day.end=yesterday }}') - toDate('{{ day.start=week_ago }}')) - 1
        )
        AND toYYYYMMDD(
            toDate('{{ day.start=week_ago }}') - 1
        )
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
) AS pre_period
