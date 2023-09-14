SELECT
    day,
    COUNT(DISTINCT dialog_id) AS cnt
FROM ods.xinghuan_dialog_tag_score_all
WHERE day BETWEEN 20230901 AND 20230911
AND shop_id = '6449efec2f2b66e59996d6db'
GROUP BY day
ORDER BY day


SELECT DISTINCT
    dialog_id
FROM ods.xinghuan_dialog_tag_score_all
WHERE day BETWEEN 20230911 AND 20230911
AND shop_id = '6449efec2f2b66e59996d6db'