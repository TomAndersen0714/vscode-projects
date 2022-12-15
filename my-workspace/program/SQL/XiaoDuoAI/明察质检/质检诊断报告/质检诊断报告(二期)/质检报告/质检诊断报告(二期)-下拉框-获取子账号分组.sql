-- 质检诊断报告(二期)-下拉框-获取子账号分组
SELECT DISTINCT
    concat(full_name,'//',_id) AS department_name_id
FROM xqc_dim.snick_department_full_all
WHERE day = toYYYYMMDD(yesterday())
-- 筛选指定企业的子账号分组
AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
AND _id GLOBAL IN (
    SELECT DISTINCT
        department_id
    FROM ods.xinghuan_qc_norm_relate_all
    WHERE day = toYYYYMMDD(yesterday())
    -- 筛选指定企业的质检标准
    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    -- 筛选指定平台
    AND platform = '{{ platform=tb }}'
    -- 下拉框-质检标准ID
    AND (
        '{{ qc_norm_ids }}'=''
        OR
        qc_norm_id IN splitByChar(',', '{{ qc_norm_ids }}')
    )
    -- 下拉框-店铺主账号
    AND (
        '{{ seller_nicks }}'=''
        OR
        seller_nick IN splitByChar(',', '{{ seller_nicks }}')
    )
)
ORDER BY department_name_id COLLATE 'zh'
