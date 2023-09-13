SELECT
    day,
    COUNT(DISTINCT dialog_id) AS cnt
FROM xqc_dwd.xplat_manual_tag_all
WHERE day BETWEEN 20230901 AND 20230909
AND shop_id = '6449efec2f2b66e59996d6db'
GROUP BY day
ORDER BY day