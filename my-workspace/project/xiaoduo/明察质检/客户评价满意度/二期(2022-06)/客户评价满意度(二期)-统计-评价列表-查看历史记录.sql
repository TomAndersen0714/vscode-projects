-- 客户评价满意度(二期)-分析-评价列表-查看历史记录
SELECT
    seller_nick,
    snick,
    cnick,
    dialog_id,
    eval_code,
    eval_time,
    send_time,
    source,
    if(eval_time != '' AND source = 1, 0, 1) AS is_invited,
    day,

    seller_nick AS `店铺`,
    snick AS `客服子账号`,
    cnick AS `顾客名称`,
    if(is_invited, send_time, '-') AS `邀评时间`,
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
FROM xqc_ods.dialog_eval_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
    AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
-- 过滤已评价数据
AND eval_time != ''
AND seller_nick = '{{ seller_nick }}'
AND snick = '{{ snick }}'
AND cnick = '{{ cnick }}'
AND send_time = '{{ send_time }}'
ORDER BY eval_time DESC