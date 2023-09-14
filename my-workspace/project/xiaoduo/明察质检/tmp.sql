SELECT
    day,
    COUNT(DISTINCT dialog_id) AS cnt
FROM xqc.public.manual_tag_record
WHERE day BETWEEN 20230901 AND 20230913
AND shop_id = '6449efec2f2b66e59996d6db'
GROUP BY day
ORDER BY day