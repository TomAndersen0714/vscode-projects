SELECT
    day,
    uniqExact(snick) AS snick_cnt
FROM xqc_dws.tag_group_stat_all
WHERE day = 20220814
AND snick GLOBAL IN (
    -- 筛选指定子账号分组中的子账号
    SELECT snick
    FROM ods.xinghuan_employee_snick_all
    WHERE day = toYYYYMMDD(yesterday())
    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    AND department_id IN (
        -- 筛选指定质检标准对应的子账号分组
        SELECT department_id
        FROM ods.xinghuan_qc_norm_relate_all
        WHERE day = toYYYYMMDD(yesterday())
        AND qc_norm_id IN splitByChar(',', '{{ qc_norm_ids }}')
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    )
)
GROUP BY day