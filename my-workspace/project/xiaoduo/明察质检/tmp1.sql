SELECT
    sumIf(
        dialog_cnt,
        day BETWEEN toYYYYMMDD(toDate('{{ day.start }}')) AND toYYYYMMDD(toDate('{{ day.end }}'))
    ) AS dialog_sum,
    sumIf(
        dialog_cnt,
        day BETWEEN toYYYYMMDD(
            toDate('{{ day.start }}') - (toDate('{{ day.end }}') - toDate('{{ day.start }}')) - 1
        )
        AND toYYYYMMDD(
            toDate('{{ day.start }}') - 1
        )
    ) AS pre_period_dialog_sum
FROM xqc_dws.snick_stat_all
WHERE day BETWEEN toYYYYMMDD(
        toDate('{{ day.start }}') - (toDate('{{ day.end }}') - toDate('{{ day.start }}')) - 1
    )
    AND toYYYYMMDD(toDate('{{ day.end }}'))
-- 筛选指定平台
AND platform = 'jd'
-- 筛选指定店铺
AND seller_nick = '九牧官方旗舰店'
-- 筛选指定子账号
AND snick GLOBAL IN (
    SELECT snick
    FROM xqc_dim.snick_full_info_all
    WHERE day = toYYYYMMDD(yesterday())
    -- 筛选指定企业
    AND company_id = '{{ company_id }}'
    -- 筛选指定平台
    AND platform = 'jd'
)