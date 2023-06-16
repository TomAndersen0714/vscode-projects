WITH dateDiff(
    'day',
    toDate('{{ day.start }}'),
    toDate('{{ day.end }}')
) + 1 AS subDays,
toDate('{{ day.start }}') AS day_start,
toDate('{{ day.end }}') AS day_end -- 取双倍周期的数据
SELECT sid,
    qids,
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
        SELECT sid,
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
                SELECT sid,
                    uniqCnt,
                    dialog_cnt,
                    rowNumberInAllBlocks() + 1 AS rank1
                FROM (
                        SELECT sid,
                            groupBitmapOr(cnick_id_bitmap) AS uniqCnt,
                            sum(dialog_sum) AS dialog_cnt
                        FROM (
                                SELECT *
                                FROM dws.voc_goods_question_stat_all
                                WHERE day BETWEEN toYYYYMMDD(day_start) AND toYYYYMMDD(day_end)
                                    AND platform = 'jd'
                            )
                        GROUP BY sid
                        ORDER BY uniqCnt DESC
                        LIMIT 100
                    )
            ) GLOBAL
            LEFT JOIN (
                SELECT sid,
                    uniqCnt2,
                    rowNumberInAllBlocks() + 1 AS rank2
                FROM (
                        SELECT sid,
                            groupBitmapOr(cnick_id_bitmap) AS uniqCnt2
                        FROM (
                                SELECT *
                                FROM dws.voc_goods_question_stat_all
                                WHERE day BETWEEN toYYYYMMDD(subtractDays(day_start, subDays)) AND toYYYYMMDD(subtractDays(day_start, 1))
                                    AND platform = 'jd' -- 下拉店铺

                            )
                        GROUP BY sid
                        ORDER BY uniqCnt2 DESC
                    )
            ) USING(sid)
    ) AS rank_info GLOBAL
    LEFT JOIN (
        SELECT sid,
            arrayStringConcat(question_b_qids, ',') as qids,
            question_b_name AS question,
            first_group_name,
            second_group_name,
            third_group_name,
            fourth_group_name
        FROM dim.voc_question_b_detail_all
        WHERE company_id = '{{ company_id }}'
    ) AS qb USING(sid)