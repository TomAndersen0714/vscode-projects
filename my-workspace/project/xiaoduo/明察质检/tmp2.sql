SELECT question_id,
    question AS `咨询场景`,
    rank1 AS `排名`,
    uniqCnt AS `人数`,
    dialog_cnt AS `声量`,
    cn_change AS `排名变化`,
    first_group_name AS `一级场景`,
    second_group_name AS `二级场景`,
    third_group_name AS `三级场景`,
    fourth_group_name AS `四级场景`
FROM (
        SELECT question_id,
            uniqCnt,
            dialog_cnt,
            rank1,
            rank2 - rank1 AS rank_change,
            IF (
                rank2 = 0
                OR rank_change = 0,
                '-',
                IF (
                    rank_change > 0,
                    concat('上升', toString(rank_change)),
                    concat('下降', toString(abs(rank_change)))
                )
            ) AS cn_change
        FROM (
                SELECT question_id,
                    uniqCnt,
                    dialog_cnt,
                    rowNumberInAllBlocks() + 1 AS rank1
                FROM (
                        SELECT question_id,
                            groupBitmapOr(cnick_id_bitmap) AS uniqCnt,
                            sum(dialog_sum) AS dialog_cnt
                        FROM (
                                SELECT *
                                FROM dws.voc_goods_question_stat_all
                                WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start }}')) AND toYYYYMMDD(toDate('{{ day.end }}'))
                                    AND platform = 'tb' -- 下拉店铺
                                    AND (
                                        '{{ goods_ids }}' = ''
                                        OR dialog_goods_id IN splitByChar(',', '{{ goods_ids }}')
                                    )
                                    AND question_id != ''
                                    AND (
                                        '60b72d421edc070017428380' = ''
                                        OR
                                        shop_id IN splitByChar(',', '60b72d421edc070017428380')
                                    )
                            )
                        GROUP BY question_id
                        ORDER BY uniqCnt DESC
                        LIMIT 100
                    )
            ) GLOBAL
            LEFT JOIN (
                SELECT question_id,
                    uniqCnt2,
                    rowNumberInAllBlocks() + 1 AS rank2
                FROM (
                        SELECT question_id,
                            groupBitmapOr(cnick_id_bitmap) AS uniqCnt2
                        FROM (
                                SELECT *
                                FROM dws.voc_goods_question_stat_all
                                WHERE day BETWEEN toYYYYMMDD(
                                        subtractDays(
                                            toDate('{{ day.start }}'),
                                            dateDiff(
                                                'day',
                                                toDate('{{ day.start }}'),
                                                toDate('{{ day.end }}')
                                            ) + 1
                                        )
                                    ) AND toYYYYMMDD(
                                        subtractDays(toDate('{{ day.start }}'), 1)
                                    )
                                    AND platform = 'tb' -- 下拉店铺
                                    AND (
                                        '{{ goods_ids }}' = ''
                                        OR dialog_goods_id IN splitByChar(',', '{{ goods_ids }}')
                                    )
                                    AND (
                                        '60b72d421edc070017428380' = ''
                                        OR
                                        shop_id IN splitByChar(',', '60b72d421edc070017428380')
                                    )
                                    AND question_id != ''
                            )
                        GROUP BY question_id
                        ORDER BY uniqCnt2 DESC
                    )
            ) USING(question_id)
    ) AS rank_info GLOBAL
    LEFT JOIN (
        SELECT DISTINCT
            question_b_qid AS question_id,
            question_b_name AS question,
            first_group_name,
            second_group_name,
            third_group_name,
            fourth_group_name
        FROM dim.voc_question_b_detail_all
        WHERE company_id = '{{ company_id=63fc50f0a06a5ecd9a249ac9 }}'
    ) AS qb
    USING(question_id)