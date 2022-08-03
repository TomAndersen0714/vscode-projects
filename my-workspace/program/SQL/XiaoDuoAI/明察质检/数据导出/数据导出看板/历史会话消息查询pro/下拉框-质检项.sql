-- 下拉框-质检项
SELECT DISTINCT
    CONCAT(name, '//', _id) AS tag_name_id
FROM xqc_dim.qc_rule_all
WHERE day = toYYYYMMDD(yesterday())
-- 下拉框-质检标准
AND qc_norm_id IN splitByChar(',','{{ qc_norm_ids }}')
-- 下拉框-质检项分组
AND (
    '{{qc_norm_group_ids}}'=''
    OR
    qc_norm_group_id IN splitByChar(',','{{ qc_norm_group_ids }}')
)
ORDER BY tag_name_id COLLATE 'zh'