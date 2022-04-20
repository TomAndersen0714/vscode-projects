SELECT
    shop_question_stat.question AS question_name,
    shop_question_stat.count AS count,
    toInt64OrZero(question_info.qid) AS qid,
    shop_question_stat.question_id AS question_id
FROM (
    SELECT question,
        question_id,
        sum(recv_count) AS count
    FROM dws.shop_snick_question_stat_all
    WHERE day BETWEEN 20220320 AND 20220419
        AND shop_id = '5e7dbfa6e4f3320016e9b7d1'
        AND question_type = 'question_b'
    GROUP BY question,
        question_id
) AS shop_question_stat
GLOBAL LEFT JOIN
    dim.question_b_v2_all AS question_info
ON shop_question_stat.question_id = question_info._id
WHERE qid != 0
    AND if(0 != 0, qid = 0, 1)
    AND if('' != '', question_name LIKE '%%', 1)
ORDER BY count DESC
LIMIT 20