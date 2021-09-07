SELECT
    t1.*,
    t2.msg_segmentation,
    t2.msg_word_class
FROM (
    SELECT 
        plat_goods_id,
        question_b_qid AS qid,
        msg AS content,
        act AS act,
        msg_time AS TIME,
        replaceAll(cnick, 'cntaobao', '') AS cnick,
        replaceAll(snick, 'cntaobao', '') AS snick,
        msg_id
    FROM
        ods.xdrs_log_all
    WHERE day = 20200825
    AND shop_id = '5a48c5c489bc46387361988d'
    AND act IN ('recv_msg','send_msg')
    ORDER BY cnick,msg_time ASC 
    limit 100
)AS t1
LEFT JOIN (
    SELECT
        msg_id,
        msg_segmentation,
        msg_word_class
    FROM 
        ods.chat_segment_v1_all
    WHERE day = 20200825
        AND shop_id = '5a48c5c489bc46387361988d'
        AND act IN ('recv_msg','send_msg')
    LIMIT 100
) AS t2
USING msg_id