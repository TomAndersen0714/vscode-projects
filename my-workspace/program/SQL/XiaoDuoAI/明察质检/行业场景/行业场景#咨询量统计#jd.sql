SELECT DISTINCT
    question AS `行业场景`,
    count AS `咨询量`,
    dim.subcategory_all.name AS `分类`,
    qid
FROM (
    SELECT 
        sq.question AS question,
        shop_stat_question.count AS count,
        sq.qid AS qid,
        sq.subcategory_id AS subcategory_id
    FROM (
        SELECT question_oid AS question_id, sum(ask_count) AS count, question
        FROM app_mp.jd_robot_shop_stat_by_question
        WHERE shop_oid = '{{ shop_id=5cac112e98ef4100118a9c9f }}'
            AND question_type=1 
            AND day BETWEEN toYYYYMMDD(parseDateTimeBestEffort('{{ day.start=month_ago }}')) AND toYYYYMMDD(parseDateTimeBestEffort('{{ day.end=yesterday }}'))
            AND question_oid GLOBAL in
            (
                SELECT _id AS question_oid FROM dim.question_b_v2_all
                WHERE
                    -- if(('{{ subcategory_id }}' != '全部' AND '{{ subcategory_id }}' != ''), subcategory_id = '{{ subcategory_id }}', subcategory_id IN (
                    --     SELECT subcategory_id FROM dim.category_subcategory_all
                    --     WHERE category_id IN (
                    --         SELECT category_id FROM dim.xdre_shop_all
                    --         WHERE _id = '{{ shop_id=5cac112e98ef4100118a9c9f }}'
                    --     )
                    -- ))
                    if(('{{ subcategory_id }}' != '全部' AND '{{ subcategory_id }}' != ''), subcategory_id = '{{ subcategory_id }}',1)
                    AND if(('{{ third_category_id }}' != '全部' AND '{{ third_category_id }}' != ''), third_category_id = '{{ third_category_id }}', 1)
                    AND if(('{{ fourth_category_id }}' != '全部' AND '{{ fourth_category_id }}' != ''), fourth_category_id = '{{ fourth_category_id }}', 1)
                    AND question_oid IN (
                    SELECT DISTINCT question_oid
                    FROM app_mp.jd_robot_shop_stat_by_question
                    WHERE day BETWEEN toYYYYMMDD(parseDateTimeBestEffort('{{ day.start=month_ago }}')) AND toYYYYMMDD(parseDateTimeBestEffort('{{ day.end=yesterday }}'))
                        AND shop_oid = '{{ shop_id=5cac112e98ef4100118a9c9f }}'
                )
            )
        GROUP BY question_id, question
        ) AS shop_stat_question
    GLOBAL LEFT JOIN dim.question_b_v2_all AS sq ON shop_stat_question.question_id = sq._id
) AS b
GLOBAL LEFT JOIN dim.subcategory_all ON b.subcategory_id = dim.subcategory_all._id    
ORDER BY `咨询量` DESC