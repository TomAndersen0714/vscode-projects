
        SELECT
            sum((100 - score + score_add) >= toUInt8OrZero('{{passing_score=100}}')) AS qualified_dialog_sum
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start }}'))
            AND toYYYYMMDD(toDate('{{ day.end }}'))
        -- 筛选指定平台
        AND platform = 'tb'
        -- 筛选指定企业的店铺
        AND seller_nick IN (
            SELECT DISTINCT
                seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day = toYYYYMMDD(yesterday())
            -- 筛选指定企业
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            -- 筛选指定平台
            AND platform = 'tb'
            -- 下拉框-店铺主账号
            AND (
                '{{ seller_nicks }}'=' '
                OR
                seller_nick IN splitByChar(',', '{{ seller_nicks }}')
            )
        )
        -- 筛选指定质检标准对应的子账号
        AND (
            '{{ qc_norm_ids }}'=' '
            OR
            snick GLOBAL IN (
                -- 筛选指定子账号分组中的子账号
                SELECT snick
                FROM xqc_dim.snick_full_info_all
                WHERE day = toYYYYMMDD(yesterday())
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                AND department_id IN (
                    -- 筛选指定质检标准对应的子账号分组
                    SELECT department_id
                    FROM ods.xinghuan_qc_norm_relate_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND qc_norm_id IN splitByChar(',', '{{ qc_norm_ids }}')
                    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                )
            )
        )
    