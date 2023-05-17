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
                                    AND question_id GLOBAL IN (
                                        SELECT question_b_qid AS question_id
                                        FROM dim.voc_question_b_detail_all
                                        WHERE company_id = '{{ company_id=63fc50f0a06a5ecd9a249ac9 }}'
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
                                            AND platform = 'tb'
                                    ) -- 当前企业对应的子账号
                                    AND snick GLOBAL IN (
                                        SELECT DISTINCT snick
                                        FROM ods.xinghuan_employee_snick_all
                                        WHERE day = toYYYYMMDD(yesterday())
                                            AND platform = 'tb'
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
                                    AND question_id != ''
                                    AND question_id GLOBAL IN (
                                        SELECT question_b_qid AS question_id
                                        FROM dim.voc_question_b_detail_all
                                        WHERE company_id = '{{ company_id=63fc50f0a06a5ecd9a249ac9 }}'
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
                                            AND platform = 'tb'
                                    ) -- 当前企业对应的子账号
                                    AND snick GLOBAL IN (
                                        SELECT DISTINCT snick
                                        FROM ods.xinghuan_employee_snick_all
                                        WHERE day = toYYYYMMDD(yesterday())
                                            AND platform = 'tb'
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
    ) AS qb USING(question_id)