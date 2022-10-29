SELECT ask_count AS hot,
    question as question_string,
    t1.question_type AS question_type,
    t1.question_id,
    subcategory_name AS category,
    msg_examples AS example_string
FROM xd_stat.presale_day_platform_snick_goods_question AS t1
    JOIN xd_stat.presale_day_platform_snick_goods_question_msg_example_distinct_by_week AS t2 ON t1.question_id = t2.question_id
WHERE t1.day between 20221022 and 20221028
    AND t1.snick_oid = '62aa7ddc1aa2d00017dcd4db'
    AND t1.plat_goods_id = '10059256512910'
    AND t2.day = 20221028
    AND t2.snick_oid = '62aa7ddc1aa2d00017dcd4db'
    AND t2.plat_goods_id = '10059256512910'
ORDER BY ask_count DESC;