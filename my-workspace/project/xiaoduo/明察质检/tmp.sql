SELECT day, COUNT(DISTINCT dialog_id) AS cnt
FROM xqc.public.manual_tag_record
WHERE day BETWEEN 20230901 AND 20230909
AND shop_id = '6449efec2f2b66e59996d6db'
GROUP BY day
ORDER BY day

SELECT dialog_id, COUNT(1) AS cnt
FROM xqc.public.manual_tag_record
WHERE day = 20230908
AND shop_id = '6449efec2f2b66e59996d6db'
GROUP BY dialog_id
HAVING COUNT(1) > 1