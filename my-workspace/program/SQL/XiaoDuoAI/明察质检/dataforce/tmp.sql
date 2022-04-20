SELECT *
FROM (
    SELECT sq.question AS question_name,
        shop_stat_question.count AS count,
        toInt64OrZero(sq.qid) AS qid,
        sq._id AS question_id
    FROM (
        SELECT question_oid AS question_id,
            sum(ask_count) AS count
        FROM app_mp.jd_robot_shop_stat_by_question
        WHERE question_type = 1
            AND day BETWEEN 20220320 AND 20220419
            AND shop_oid = '5e7dbfa6e4f3320016e9b7d1'
        GROUP BY question_id
    ) AS shop_stat_question
    GLOBAL LEFT JOIN dim.question_b_v2_all AS sq
    ON shop_stat_question.question_id = sq._id
    WHERE qid != 0
        AND if(0 != 0, qid = 0, 1)
        AND if('' != '', question_name LIKE '%%', 1)
)
ORDER BY count DESC
LIMIT 20