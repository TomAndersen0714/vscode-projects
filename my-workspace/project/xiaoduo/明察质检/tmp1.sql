WITH dateDiff(
    'day',
    toDate('{{ day.start=week_ago }}'),
    toDate('{{ day.end=yesterday }}')
) + 1 AS subDays,
toDate('{{ day.start=week_ago }}') AS day_start,
toDate('{{ day.end=yesterday }}') AS day_end -- 取双倍周期的数据
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
                                    AND (
                                        '{{ goods_ids }}' = ''
                                        OR dialog_goods_id IN splitByChar(',', '{{ goods_ids }}')
                                    )
                                    AND sid != ''
                                    AND sid GLOBAL IN (
                                        SELECT sid
                                        FROM dim.voc_question_b_detail_all
                                        WHERE company_id = '{{ company_id=63fc50f0a06a5ecd9a249ac9 }}'
                                            AND first_group_name != '未分组'
                                            AND (
                                                '{{ first_group_ids }}' = ''
                                                OR first_group_id IN splitByChar(',', '{{ first_group_ids }}')
                                            )
                                            AND (
                                                '{{ second_group_ids }}' = ''
                                                OR second_group_id IN splitByChar(',', '{{ second_group_ids }}')
                                            )
                                            AND (
                                                '{{ third_group_ids }}' = ''
                                                OR third_group_id IN splitByChar(',', '{{ third_group_ids }}')
                                            )
                                            AND (
                                                '{{ fourth_group_ids }}' = ''
                                                OR fourth_group_id IN splitByChar(',', '{{ fourth_group_ids }}')
                                            )
                                    )
                                    AND (
                                        '{{ shop_ids }}' = ''
                                        OR shop_id IN splitByChar(',', '{{ shop_ids }}')
                                    ) -- 下拉订单状态
                                    -- 下拉订单状态
                                    AND (
                                        '{{ order_status }}' = ''
                                        OR recent_order_status IN splitByChar(
                                            ',',
                                            replaceAll('{{ order_status }}', 'unorder', '')
                                        )
                                    )
                                    AND (
                                        '{{ round_count }}' = ''
                                        OR toString(dialog_qa_stage) IN splitByChar(',', '{{ round_count }}')
                                    ) -- 当前企业对应的店铺
                                    AND shop_id GLOBAL IN (
                                        SELECT DISTINCT shop_id
                                        FROM xqc_dim.xqc_shop_all
                                        WHERE day = toYYYYMMDD(yesterday())
                                            AND company_id = '{{ company_id=63fc50f0a06a5ecd9a249ac9 }}'
                                            AND platform = 'jd'
                                    ) -- 当前企业对应的子账号
                                    AND snick GLOBAL IN (
                                        SELECT DISTINCT snick
                                        FROM ods.xinghuan_employee_snick_all
                                        WHERE day = toYYYYMMDD(yesterday())
                                            AND platform = 'jd'
                                            AND company_id = '{{ company_id=63fc50f0a06a5ecd9a249ac9 }}' -- 下拉框-子账号分组id
                                            AND (
                                                '{{ department_ids }}' = ''
                                                OR department_id IN splitByChar(',', '{{ department_ids }}')
                                            ) -- 下拉子账号
                                            AND (
                                                '{{ snicks }}' = ''
                                                OR snick IN splitByChar(',', '{{ snicks }}')
                                            )
                                    )
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
                                    AND (
                                        '{{ goods_ids }}' = ''
                                        OR dialog_goods_id IN splitByChar(',', '{{ goods_ids }}')
                                    )
                                    AND (
                                        '{{ shop_ids }}' = ''
                                        OR shop_id IN splitByChar(',', '{{ shop_ids }}')
                                    ) -- 下拉订单状态
                                    -- 下拉订单状态
                                    AND (
                                        '{{ order_status }}' = ''
                                        OR recent_order_status IN splitByChar(
                                            ',',
                                            replaceAll('{{ order_status }}', 'unorder', '')
                                        )
                                    )
                                    AND (
                                        '{{ round_count }}' = ''
                                        OR toString(dialog_qa_stage) IN splitByChar(',', '{{ round_count }}')
                                    ) -- 当前企业对应的店铺
                                    AND sid != ''
                                    AND sid GLOBAL IN (
                                        SELECT sid 
                                        FROM dim.voc_question_b_detail_all
                                        WHERE company_id = '{{ company_id=63fc50f0a06a5ecd9a249ac9 }}'
                                            AND first_group_name != '未分组'
                                            AND (
                                                '{{ first_group_ids }}' = ''
                                                OR first_group_id IN splitByChar(',', '{{ first_group_ids }}')
                                            )
                                            AND (
                                                '{{ second_group_ids }}' = ''
                                                OR second_group_id IN splitByChar(',', '{{ second_group_ids }}')
                                            )
                                            AND (
                                                '{{ third_group_ids }}' = ''
                                                OR third_group_id IN splitByChar(',', '{{ third_group_ids }}')
                                            )
                                            AND (
                                                '{{ fourth_group_ids }}' = ''
                                                OR fourth_group_id IN splitByChar(',', '{{ fourth_group_ids }}')
                                            )
                                    )
                                    AND shop_id GLOBAL IN (
                                        SELECT DISTINCT shop_id
                                        FROM xqc_dim.xqc_shop_all
                                        WHERE day = toYYYYMMDD(yesterday())
                                            AND company_id = '{{ company_id=63fc50f0a06a5ecd9a249ac9 }}'
                                            AND platform = 'jd'
                                    ) -- 当前企业对应的子账号
                                    AND snick GLOBAL IN (
                                        SELECT DISTINCT snick
                                        FROM ods.xinghuan_employee_snick_all
                                        WHERE day = toYYYYMMDD(yesterday())
                                            AND platform = 'jd'
                                            AND company_id = '{{ company_id=63fc50f0a06a5ecd9a249ac9 }}' -- 下拉框-子账号分组id
                                            AND (
                                                '{{ department_ids }}' = ''
                                                OR department_id IN splitByChar(',', '{{ department_ids }}')
                                            ) -- 下拉子账号
                                            AND (
                                                '{{ snicks }}' = ''
                                                OR snick IN splitByChar(',', '{{ snicks }}')
                                            )
                                    )
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
        WHERE company_id = '{{ company_id=63fc50f0a06a5ecd9a249ac9 }}'
            AND first_group_name != '未分组'
            AND (
                '{{ first_group_ids }}' = ''
                OR first_group_id IN splitByChar(',', '{{ first_group_ids }}')
            )
            AND (
                '{{ second_group_ids }}' = ''
                OR second_group_id IN splitByChar(',', '{{ second_group_ids }}')
            )
            AND (
                '{{ third_group_ids }}' = ''
                OR third_group_id IN splitByChar(',', '{{ third_group_ids }}')
            )
            AND (
                '{{ fourth_group_ids }}' = ''
                OR fourth_group_id IN splitByChar(',', '{{ fourth_group_ids }}')
            )
    ) AS qb USING(sid)