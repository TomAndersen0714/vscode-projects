SELECT
    day,
    snick,
    COUNT(1)
FROM (
    SELECT DISTINCT
        day, dialog_id, snick, cnick
    FROM xqc_dwd.xplat_manual_tag_all
    WHERE platform = 'tb'
    AND company_id = '644a17ff8dc175f38f3dd916'
    AND day = 20230909
)
GROUP BY day, snick
ORDER BY day, snick