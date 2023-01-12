-- 质检诊断报告(二期)-下拉框-获取店铺主账号
SELECT DISTINCT
    seller_nick
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
-- 下拉框-子账号分组ID
AND (
    '{{ department_ids }}'=''
    OR
    department_id IN splitByChar(',','{{ department_ids }}')
)
ORDER BY seller_nick COLLATE 'zh'