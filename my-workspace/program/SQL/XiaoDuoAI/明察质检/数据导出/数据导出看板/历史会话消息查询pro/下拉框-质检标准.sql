-- 下拉框-质检标准
SELECT DISTINCT
    CONCAT(name, '//', _id) AS qc_norm_name_id
FROM ods.xinghuan_qc_norm_all
WHERE day = toYYYYMMDD(yesterday())
AND _id GLOBAL IN (
    SELECT DISTINCT
        qc_norm_id
    FROM ods.xinghuan_qc_norm_relate_all
    WHERE day = toYYYYMMDD(yesterday())
    -- 下拉框-平台
    AND platform = '{{ platform=tb }}'
    -- 下拉框-店铺主账号
    AND seller_nick = '{{ seller_nick=方太官方旗舰店 }}'
) AS qc_norm_ids
ORDER BY qc_norm_name_id COLLATE 'zh'