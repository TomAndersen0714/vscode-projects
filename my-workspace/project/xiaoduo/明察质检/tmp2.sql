-- 质检诊断报告(二期)-客服质检报告-店铺会话合格率
SELECT
    seller_nick,
    uniqExact(_id) AS dialog_cnt,
    sum((100 - score + score_add) >= toUInt8OrZero('{{ passing_score }}')) AS qualified_dialog_cnt,
    if(dialog_cnt!=0, round(qualified_dialog_cnt/dialog_cnt*100, 2), 0.00) AS qualified_dialog_pct,
    seller_nick AS `店铺主账号`,
    dialog_cnt AS `质检会话量`,
    CONCAT(toString(qualified_dialog_pct), '%') AS `会话合格率`
FROM dwd.xdqc_dialog_all
WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start }}'))
    AND toYYYYMMDD(toDate('{{ day.end }}'))
-- 筛选指定平台
AND platform = 'jd'
-- 筛选指定店铺
AND seller_nick = '九牧官方旗舰店'
GROUP BY seller_nick
ORDER BY seller_nick COLLATE 'zh'