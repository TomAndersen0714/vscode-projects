SELECT question AS `咨询场景`,
    uniqCnt AS `人数`,
    first_group_name AS `一级场景`,
    second_group_name AS `二级场景`,
    third_group_name AS `三级场景`,
    fourth_group_name AS `四级场景`
FROM (
        SELECT
            question_id,
            question,
            uniqCnt,
            dialog_cnt
        FROM (
            SELECT question_id,
                groupBitmapOr(cnick_id_bitmap) AS uniqCnt,
                sum(dialog_sum) AS dialog_cnt
            FROM (
                    SELECT *
                    FROM dws.voc_goods_question_stat_all
                    WHERE day BETWEEN toYYYYMMDD(toDate('{{day_start}}')) AND toYYYYMMDD(toDate('{{day_end}}'))
                        AND platform = 'jd'
                        AND question_id != ''
                        AND shop_id GLOBAL IN (
                            SELECT DISTINCT shop_id
                            FROM xqc_dim.xqc_shop_all
                            WHERE day = toYYYYMMDD(yesterday())
                                AND company_id = '{{ company_id=63fc50f0a06a5ecd9a249ac9 }}'
                                AND platform = 'jd' 
                        )
                )
            GROUP BY question_id
        ) AS question_stat_info
        LEFT JOIN (
            SELECT DISTINCT
                qid AS question_id,
                question
            FROM dim.question_b_all
        ) AS question_all_info
        USING(question_id)
        ORDER BY uniqCnt DESC
    ) AS rank_info
    GLOBAL LEFT JOIN (
        SELECT question_b_name AS question,
            first_group_name,
            second_group_name,
            third_group_name,
            fourth_group_name
        FROM dim.voc_question_b_detail_all
        WHERE company_id = '{{ company_id=63fc50f0a06a5ecd9a249ac9 }}'
    ) AS qb
    USING(question)
Order by `一级场景`,
    `二级场景`,
    `三级场景`,
    `四级场景`