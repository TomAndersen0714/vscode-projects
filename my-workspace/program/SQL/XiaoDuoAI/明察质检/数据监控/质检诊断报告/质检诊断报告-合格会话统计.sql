-- 质检诊断报告-合格会话统计
WITH (
    SELECT toDate('{{ day.end=yesterday }}') - toDate('{{ day.start=week_ago }}')
)
SELECT
    COUNT(1) AS dialog_cnt,
    SUM((100 - score + score_add) >= 60) AS qualified_dialog_cnt
FROM dwd.xdqc_dialog_all
WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
    AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
-- 筛选指定平台
AND platform = '{{ platform=tb }}'
-- 筛选指定企业的店铺
AND seller_nick IN (
    SELECT DISTINCT
        seller_nick
    FROM xqc_dim.xqc_shop_all
    WHERE day = toYYYYMMDD(yesterday())
    -- 筛选指定企业
    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    -- 筛选指定平台
    AND platform = '{{ platform=tb }}'
    -- 下拉框-店铺主账号
    AND (
        '{{ seller_nicks }}'=''
        OR
        seller_nick IN splitByChar(',', '{{ seller_nicks }}')
    )
)
