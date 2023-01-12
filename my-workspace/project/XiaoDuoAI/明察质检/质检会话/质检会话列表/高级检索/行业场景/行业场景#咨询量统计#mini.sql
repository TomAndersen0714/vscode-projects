SELECT DISTINCT
    question AS `行业场景`,
    count AS `咨询量`,
    dim.subcategory_all.name AS `分类`,
    qid
FROM (
    SELECT
        count,question,sq.qid AS qid,
        sq.subcategory_id AS subcategory_id
    FROM (
        SELECT 
            sum(recv_count) AS count,
            question, question_id
        FROM app_mp.shop_question_stat_all
        WHERE 
            day BETWEEN toYYYYMMDD(parseDateTimeBestEffort('{{ day.start=month_ago }}')) AND toYYYYMMDD(parseDateTimeBestEffort('{{ day.end=yesterday }}'))
            AND shop_id = '{{ shop_id=5cac112e98ef4100118a9c9f }}'
            AND question_id GLOBAL IN
            (
                SELECT _id AS question_id FROM dim.question_b_v2_all
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
                    AND _id IN (
                    SELECT DISTINCT question_id
                    FROM app_mp.shop_question_stat_all
                    WHERE day BETWEEN toYYYYMMDD(parseDateTimeBestEffort('{{ day.start=month_ago }}')) AND toYYYYMMDD(parseDateTimeBestEffort('{{ day.end=yesterday }}'))
                        AND shop_id = '{{ shop_id=5cac112e98ef4100118a9c9f }}'
                )
            )
        GROUP BY question_id, question
    ) AS question_status
    GLOBAL LEFT JOIN dim.question_b_v2_all AS sq ON question_status.question_id = sq._id
) AS b
GLOBAL LEFT JOIN dim.subcategory_all ON b.subcategory_id = dim.subcategory_all._id
ORDER BY `咨询量` DESC