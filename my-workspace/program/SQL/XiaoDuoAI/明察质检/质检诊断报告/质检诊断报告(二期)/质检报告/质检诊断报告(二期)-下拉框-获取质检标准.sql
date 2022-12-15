-- 质检诊断报告(二期)-下拉框-获取质检标准
SELECT DISTINCT
    CONCAT(name, '//', _id) AS qc_norm_name_id
FROM ods.xinghuan_qc_norm_all
WHERE day = toYYYYMMDD(yesterday())
-- 筛选指定企业的质检标准
AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
-- 仅筛选开启状态的质检标准
AND status = 1
-- 筛选绑定指定店铺的质检标准ID
AND _id GLOBAL IN (
    SELECT DISTINCT
        qc_norm_id
    FROM ods.xinghuan_qc_norm_relate_all
    WHERE day = toYYYYMMDD(yesterday())
    -- 筛选指定企业的质检标准
    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    -- 筛选指定平台
    AND platform = '{{ platform=tb }}'
    -- 下拉框-店铺主账号
    AND (
        '{{ seller_nicks }}'=''
        OR
        seller_nick IN splitByChar(',', '{{ seller_nicks }}')
    )
    -- 下拉框-子账号分组ID
    AND (
        '{{ department_ids }}'=''
        OR
        department_id IN splitByChar(',','{{ department_ids }}')
    )
) AS qc_norm_ids
ORDER BY qc_norm_name_id COLLATE 'zh'