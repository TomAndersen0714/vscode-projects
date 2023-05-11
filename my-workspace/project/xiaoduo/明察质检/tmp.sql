SELECT
    plat_goods_id,
    plat_goods_name,
    question_b_qid,
    question_b_standard_q,
    precise_intent_id,
    precise_intent_standard_q,
    msg_sample,
    msg_cnt
FROM (
    SELECT
        question_b_qid,
        question_b_standard_q,
        precise_intent_id,
        precise_intent_standard_q,
        plat_goods_id,
        arraySlice(groupArray(msg), 1, 5) AS msg_sample,
        COUNT(1) AS msg_cnt
    FROM ods.xdrs_logs_all
    WHERE day BETWEEN 
        AND toYYYYMMDD(toDate('{{date_time}}'))
    AND shop_id = '{{ shop_id }}'
    -- 筛选接收消息
    AND act = 'recv_msg'
    -- 筛选命中精准意图的消息
    AND precise_intent_id !=''
    -- 筛选指定商品
    AND plat_goods_id IN (
        SELECT
            plat_goods_id
        FROM dim.goods_center_all
        WHERE platform = 'tb'
        AND shop_id = '{{ shop_id }}'
        -- 筛选商品关键词
        AND plat_goods_name LIKE '%{{goods_class}}%'
    )
    GROUP BY
        question_b_qid,
        question_b_standard_q,
        precise_intent_id,
        precise_intent_standard_q,
        plat_goods_id
    HAVING
        -- 筛选咨询量大于0的
        msg_cnt > 0
) AS log_info
LEFT JOIN (
    SELECT
        plat_goods_id,
        plat_goods_name
    FROM dim.goods_center_all
    WHERE platform = 'tb'
    AND shop_id = '{{ shop_id }}'
    AND plat_goods_name LIKE '%{{goods_class}}%'
) AS goods_info
USING(plat_goods_id)