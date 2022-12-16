-- 质检诊断报告(二期)-客服质检报告-客服会话合格率
SELECT
    rowNumberInAllBlocks()+1 AS `排名`,
    snick AS `子账号`,
    employee_id,
    employee_name AS `客服姓名`,
    dialog_cnt AS `质检会话量`,
    qualified_dialog_cnt AS `合格会话量`,
    qualified_dialog_pct_str AS `会话合格率`
FROM (
    SELECT
        snick,
        COUNT(1) AS dialog_cnt,
        sum((100 - score + score_add) >= toUInt8OrZero('{{ passing_score=100 }}')) AS qualified_dialog_cnt,
        IF(dialog_cnt!=0, round(qualified_dialog_cnt / dialog_cnt * 100, 2), 0.00) AS qualified_dialog_pct,
        CONCAT(toString(qualified_dialog_pct),'%') AS qualified_dialog_pct_str
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
        AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
    -- 筛选指定平台
    AND platform = '{{ platform }}'
    -- 筛选指定店铺
    AND seller_nick GLOBAL IN (
        SELECT DISTINCT
            seller_nick
        FROM xqc_dim.xqc_shop_all
        WHERE day = toYYYYMMDD(yesterday())
        -- 筛选指定企业
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        -- 筛选指定平台
        AND platform = '{{ platform }}'
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
                AND platform = '{{ platform }}'
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
        AND platform = '{{ platform }}'
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
                AND platform = '{{ platform }}'
                -- 下拉框-质检标准ID
                AND qc_norm_id IN splitByChar(',', '{{ qc_norm_ids }}')
            )
        )
    )
    GROUP BY snick
    ORDER BY qualified_dialog_pct_str, snick DESC COLLATE 'zh'
) AS snick_stat
GLOBAL INNER JOIN (
    -- 获取子账号信息
    SELECT snick, employee_id, employee_name
    FROM xqc_dim.snick_full_info_all
    WHERE day = toYYYYMMDD(yesterday())
    -- 筛选指定企业
    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    -- 筛选指定平台
    AND platform = '{{ platform }}'
    -- 剔除未绑定员工的子账号
    AND employee_name!=''
) AS snick_info
USING(snick)
ORDER BY qualified_dialog_pct_str, snick DESC COLLATE 'zh'