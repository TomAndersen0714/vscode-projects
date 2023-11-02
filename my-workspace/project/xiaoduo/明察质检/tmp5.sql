SELECT
    platform, seller_nick, snick, tag_type, tag_id, tag_name,
    sum(tag_cnt_sum) AS tag_cnt_sum
FROM xqc_dws.tag_stat_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start }}')) AND toYYYYMMDD(toDate('{{ day.end }}'))
AND platform = 'jd'
AND seller_nick GLOBAL IN (
    -- 查询对应企业-平台的店铺
    SELECT DISTINCT seller_nick
    FROM xqc_dim.xqc_shop_all
    WHERE day=toYYYYMMDD(yesterday())
    AND platform = 'jd'
    AND company_id = '61602afd297bb79b69c06118'
)
AND snick GLOBAL IN (
    -- 获取最新版本的维度数据(T+1)
    SELECT distinct snick
    FROM ods.xinghuan_employee_snick_all
    WHERE day = toYYYYMMDD(yesterday())
    AND platform = 'jd'
    AND company_id = '61602afd297bb79b69c06118'
)
AND tag_id = '641d8b61dc72ebff92f598f3'
GROUP BY platform, seller_nick, snick, tag_type, tag_id, tag_name