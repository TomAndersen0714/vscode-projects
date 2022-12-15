-- 质检诊断报告(二期)-会话质检报告(人工)-质检场景诊断(一级)
SELECT
    qc_norm_info.tag_group_name AS `质检场景`,
    tag_group_stat.subtract_score_dialog_sum AS `扣分会话数`
FROM (
    SELECT
        qc_norm_id,
        tag_group_id,
        SUM(add_score_dialog_cnt) AS add_score_dialog_sum,
        -- subtract_score_dialog_cnt 待替换 subtract_score_manual_dialog_cnt
        SUM(subtract_score_dialog_cnt) AS subtract_score_dialog_sum
    FROM xqc_dws.tag_group_stat_all
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
    )
    -- 下拉框-质检标准
    AND (
        '{{ qc_norm_ids }}'=''
        OR
        qc_norm_id IN splitByChar(',', '{{ qc_norm_ids }}')
    )
    -- 筛选一级质检项分组
    AND tag_group_level = 1
    GROUP BY
        qc_norm_id,
        tag_group_id
    ORDER BY qc_norm_id
) AS tag_group_stat
GLOBAL LEFT JOIN (
    SELECT
        qc_norm_id,
        _id AS tag_group_id,
        name AS tag_group_name
    FROM xqc_dim.qc_norm_group_all
    WHERE day = toYYYYMMDD(yesterday())
    -- 下拉框-质检标准
    AND (
        '{{ qc_norm_ids }}'=''
        OR
        qc_norm_id IN splitByChar(',', '{{ qc_norm_ids }}')
    )
) qc_norm_info
USING(qc_norm_id, tag_group_id)
-- 选择质检标准时, 展示所有, 不选择质检标准时过滤扣分会话量为0的分组, 避免展示分组数量太多
WHERE subtract_score_dialog_sum != 0
ORDER BY qc_norm_id, tag_group_id