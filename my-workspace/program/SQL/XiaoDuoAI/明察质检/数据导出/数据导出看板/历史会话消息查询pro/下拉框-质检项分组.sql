-- 下拉框-质检项分组
SELECT DISTINCT
    CONCAT(full_name, '//', _id) AS qc_norm_group_name_id
FROM xqc_dim.qc_norm_group_full_all
WHERE day = toYYYYMMDD(yesterday())
AND _id GLOBAL IN (
    -- 查询质检项所在末级分组ID
    SELECT
        qc_norm_group_id
    FROM xqc_dim.qc_rule_all
    WHERE day = toYYYYMMDD(yesterday())
    -- 下拉框-质检标准
    AND qc_norm_id IN splitByChar(',','{{ qc_norm_ids }}')
)
ORDER BY qc_norm_group_name_id COLLATE 'zh'