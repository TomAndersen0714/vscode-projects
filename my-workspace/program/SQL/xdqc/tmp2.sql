SELECT
    *
FROM (
    SELECT
        cnick, 
        snick, 
        if(source=1, 'send_msg', 'recv_msg')  AS act,
        content,
        toDateTime64(create_time,0)+8*3600,
        excellent AS qc_id
    FROM xqc_ods.message_all
    ARRAY JOIN
        excellent
    WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start}}')) AND toYYYYMMDD(toDate('{{day.end}}'))
    AND source = if('{{source}}'='顾客',0,1)
    Limit {{limit}}
) AS message_info
GLOBAL LEFT JOIN (
SELECT
    toInt32(_qc_id) AS qc_id,
    qc_name
FROM numbers(1)
ARRAY JOIN
    [1,2,3,4,5,6,7,8,9,10,11,12,13,14] AS _qc_id,
    ['需求挖掘','商品细节解答','卖点传达','商品推荐',
    '退换货理由修改','主动跟进','无货挽回','活动传达',
    '店铺保障','催拍催付','核对地址','好评引导','优秀结束语','试听课跟单'] AS qc_name
) AS qc_info
USING(qc_id)