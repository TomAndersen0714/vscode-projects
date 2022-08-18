-- 质检诊断报告-会话-质检二级分组场景诊断
SELECT
    qc_norm_info.tag_group_name AS `质检场景`,
    tag_group_stat.subtract_score_dialog_sum AS `扣分会话数`
FROM (
    SELECT
        qc_norm_id,
        tag_group_id,
        SUM(subtract_score_dialog_cnt) AS subtract_score_dialog_sum
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
    -- 筛选二级质检项分组
    AND tag_group_level = 2
    -- 不展示没有二级质检分组的数据
    AND tag_group_id != ''
    -- 筛选指定一级质检项分组下的二级质检项分组
    AND (
        '{{ tag_group_ids }}'='all'
        OR
        tag_group_id GLOBAL IN (
            SELECT
                _id AS tag_group_id
            FROM xqc_dim.qc_norm_group_full_all
            WHERE day = toYYYYMMDD(yesterday())
            -- 下拉框-一级质检项分组
            AND parent_id IN splitByChar(',', '{{ tag_group_ids }}')
        )
    )
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
