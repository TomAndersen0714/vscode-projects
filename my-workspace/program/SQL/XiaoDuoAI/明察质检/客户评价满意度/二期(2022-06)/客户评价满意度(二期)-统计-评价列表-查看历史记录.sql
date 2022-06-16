-- 客户评价满意度(二期)-分析-评价列表-查看历史记录
SELECT
    replaceOne(splitByChar(':', user_nick)[1], 'cntaobao','') AS seller_nick,
    replaceOne(user_nick, 'cntaobao', '') AS snick,
    replaceOne(eval_recer, 'cntaobao', '') AS cnick,
    source,
    send_time,
    eval_time,
    eval_code,

    seller_nick AS `店铺`,
    snick AS `客服子账号`,
    cnick AS `顾客名称`,
    if(source!=1, send_time, '') AS `邀评时间`,
    eval_time AS `评价时间`,
    CASE
        WHEN source=0 THEN '客服邀评'
        WHEN source=1 THEN '消费者自主评价'
        WHEN source=2 THEN '系统邀评'
        ELSE ''
    END AS `评价来源`,
    CASE
        WHEN eval_code=0 THEN '非常满意'
        WHEN eval_code=1 THEN '满意'
        WHEN eval_code=2 THEN '一般'
        WHEN eval_code=3 THEN '不满意'
        WHEN eval_code=4 THEN '非常不满意'
        ELSE ''
    END AS `评价结果`
FROM xqc_ods.snick_eval_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
    AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
AND seller_nick = '{{ seller_nick }}'
AND user_nick = 'cntaobao{{ snick }}'
AND eval_recer = 'cntaobao{{ cnick }}'
AND send_time = '{{ send_time }}'
-- 过滤已有评价
AND eval_time != ''
ORDER BY eval_time DESC