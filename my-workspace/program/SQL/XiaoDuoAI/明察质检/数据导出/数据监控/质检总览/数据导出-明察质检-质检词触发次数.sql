-- 数据导出-明察质检-质检词触发次数
SELECT
    platform AS `平台`,
    platform_qc_word_stat.word_cnt AS `质检词总数`,
    qc_word_stat.word AS `质检词`,
    qc_word_stat.word_cnt AS `质检词触发次数`,
    CONCAT(toString(if(`质检词总数`!=0, round(`质检词触发次数`/`质检词总数`*100,2), 0.00)),'%') AS `质检词次数占比`
FROM (
    SELECT
        platform,
        word,
        sum(words_count) AS word_cnt
    FROM ods.qc_words_detail_all
    WHERE toYYYYMMDD(date) BETWEEN toYYYYMMDD(toDate('{{ day.start=yesterday }}'))AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
        -- 获取指定企业的店铺
        AND shop_name GLOBAL IN (
            SELECT DISTINCT seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        )
        -- 下拉框-平台
        AND platform = 'tb'
        -- 下拉框-店铺名
        AND (
            '{{ seller_nicks }}'=''
            OR
            shop_name IN splitByChar(',','{{ seller_nicks }}')
        )
    GROUP BY platform, word
    ORDER BY word_cnt DESC
) AS qc_word_stat
GLOBAL INNER JOIN (
    SELECT
        platform,
        sum(words_count) AS word_cnt
    FROM ods.qc_words_detail_all
    WHERE toYYYYMMDD(date) BETWEEN toYYYYMMDD(toDate('{{ day.start=yesterday }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
        -- 获取指定企业的店铺
        AND shop_name GLOBAL IN (
            SELECT DISTINCT seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        )
        -- 下拉框-平台
        AND platform = 'tb'
        -- 下拉框-店铺名
        AND (
            '{{ seller_nicks }}'=''
            OR
            shop_name IN splitByChar(',','{{ seller_nicks }}')
        )
    GROUP BY platform
    ORDER BY word_cnt DESC
) AS platform_qc_word_stat
USING(platform)
ORDER BY word_cnt DESC