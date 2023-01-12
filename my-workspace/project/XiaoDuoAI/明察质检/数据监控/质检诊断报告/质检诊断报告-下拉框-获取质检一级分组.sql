-- 质检诊断报告-会话-下拉框-获取质检一级分组
SELECT
    '全部//all' AS qc_norm_group_name_id
UNION ALL
SELECT
    CONCAT(qc_norm_info.tag_group_name, '//', qc_norm_info.tag_group_id) AS qc_norm_group_name_id
FROM (
    SELECT
        qc_norm_id,
        tag_group_id,
        SUM(add_score_dialog_cnt) AS add_score_dialog_sum,
        SUM(subtract_score_dialog_cnt) AS subtract_score_dialog_sum
    FROM xqc_dws.tag_group_stat_all
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
    FROM remote('10.22.134.218:19000', xqc_dim.qc_norm_group_all)
    WHERE day = toYYYYMMDD(yesterday())
    -- 下拉框-质检标准
    AND (
        '{{ qc_norm_ids }}'=''
        OR
        qc_norm_id IN splitByChar(',', '{{ qc_norm_ids }}')
    )
) AS qc_norm_info
USING(qc_norm_id, tag_group_id)
-- 过滤扣分会话量为0的分组, 避免展示分组数量太多
WHERE subtract_score_dialog_sum != 0
ORDER BY qc_norm_id, tag_group_id
