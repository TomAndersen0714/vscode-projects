-- 质检诊断报告(二期)-客服质检报告(人工)-店铺会话合格率
SELECT
    seller_nick,
    uniqExact(_id) AS dialog_cnt,
    sum((100 - score + score_add) >= toUInt8OrZero('{{ passing_score=100 }}')) AS qualified_dialog_cnt,
    if(dialog_cnt!=0, round(qualified_dialog_cnt/dialog_cnt*100, 2), 0.00) AS qualified_dialog_pct,
    seller_nick AS `店铺主账号`,
    dialog_cnt AS `质检会话量`,
    CONCAT(toString(qualified_dialog_pct), '%') AS `会话合格率`
FROM dwd.xdqc_dialog_all
WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
    AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
-- 筛选人工质检过的会话
AND notEmpty(last_mark_id)
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
GROUP BY seller_nick
ORDER BY seller_nick COLLATE 'zh'