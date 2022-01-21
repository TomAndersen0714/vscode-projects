SELECT DISTINCT question AS `行业场景`,
    count AS `咨询量`,
    dim.subcategory_all.name AS `分类`,
    qid
FROM (
        SELECT sq.question AS question,
            shop_stat_question.count AS count,
            sq.qid AS qid,
            sq.subcategory_id AS subcategory_id
        FROM (
                SELECT question_oid AS question_id,
                    sum(ask_count) AS count,
                    question
                FROM app_mp.jd_robot_shop_stat_by_question
                WHERE shop_oid = '{{ shop_id=61839cb5f393ab0018592ca3 }}'
                    AND question_type = 1
                    AND day BETWEEN toYYYYMMDD(
                        parseDateTimeBestEffort('{{ day.start }}')
                    ) AND toYYYYMMDD(
                        parseDateTimeBestEffort('{{ day.end }}')
                    )
                    AND question_oid in (
                        SELECT _id AS question_oid
                        FROM dim.question_b_v2
                        WHERE if(
                                (
                                    '{{ subcategory_id }}' != '全部'
                                    AND '{{ subcategory_id }}' != ''
                                ),
                                subcategory_id = '{{ subcategory_id }}',
                                1
                            )
                            AND if(
                                '{{ question }}' != '',
                                question LIKE '%{{ question }}%',
                                1
                            )
                            AND if(
                                (
                                    '{{ third_category_id }}' != '全部'
                                    AND '{{ third_category_id }}' != ''
                                ),
                                third_category_id = '{{ third_category_id }}',
                                1
                            )
                            AND if(
                                (
                                    '{{ fourth_category_id }}' != '全部'
                                    AND '{{ fourth_category_id }}' != ''
                                ),
                                fourth_category_id = '{{ fourth_category_id }}',
                                1
                            )
                    )
                GROUP BY question_id,
                    question
            ) AS shop_stat_question
            LEFT JOIN dim.question_b_v2 AS sq ON shop_stat_question.question_id = sq._id
    ) AS b
    LEFT JOIN dim.subcategory_all ON b.subcategory_id = dim.subcategory_all._id
ORDER BY `咨询量` DESC
