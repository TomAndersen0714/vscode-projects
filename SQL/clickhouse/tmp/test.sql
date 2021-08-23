
SELECT 
    question, question_id, qoid, pv
FROM (
    SELECT qid, question
    FROM dim.question_b
) AS dim
RIGHT JOIN(
    SELECT 
        question_id, qoid, pv
    FROM 
        app_mp.stat_question_for_shop
    WHERE 
        day = CAST(replace('{{ day }}','-','') AS INT)
        AND snick_oid = '{{ shop_id }}'
    ORDER BY pv DESC
    LIMIT 20
) AS ods
ON dim.qid = ods.qoid