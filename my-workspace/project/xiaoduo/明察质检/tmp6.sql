INSERT INTO dwd.voc_chat_log_detail_all
SELECT
    day,
    platform,
    shop_id,
    snick,
    cnick,
    cnick_id,
    real_buyer_nick,
    msg_timestamp,
    msg_id,
    msg,
    act,
    send_msg_from,
    question_b_qid,
    plat_goods_id,
    IF(recent_order_status!='', latest_order_info.order_id, '') AS recent_order_id,
    arrayFilter(
        (x)-> x<=msg_timestamp,
        latest_order_info.order_status_timestamps
    )[-1] AS recent_order_status_timestamp,
    arrayFilter(
        (x,y)-> y<=msg_timestamp,
        latest_order_info.order_statuses,
        latest_order_info.order_status_timestamps
    )[-1] AS recent_order_status,
    dialog_qa_cnt AS dialog_qa_sum
FROM (
    SELECT
        day,
        platform,
        shop_id,
        snick,
        cnick,
        cnick_id,
        real_buyer_nick,
        msg_timestamp,
        msg_id,
        msg,
        act,
        send_msg_from,
        question_b_qid,
        plat_goods_id,
        dialog_detail_info.dialog_qa_cnt
    FROM (
        -- stage_1, 获取当天的聊天消息记录
        SELECT
            day,
            platform,
            shop_id,
            replaceOne(snick,'cntaobao','') AS snick,
            replaceOne(cnick,'cntaobao','') AS cnick,
            '' AS real_buyer_nick,
            toUInt64(msg_time) AS msg_timestamp,
            msg_id,
            msg,
            act,
            send_msg_from,
            question_b_qid,
            plat_goods_id
        FROM ods.xdrs_logs_all
        WHERE day = 20230330
        AND shop_id IN ['60b72d421edc070017428380']
        AND act IN ['send_msg', 'recv_msg']
    ) AS xdrs_logs
    LEFT JOIN (
        SELECT
            day,
            platform,
            shop_id,
            snick,
            cnick,
            cnick_info.cnick_id,
            real_buyer_nick,
            dialog_qa_cnt
        FROM (
            -- stage_2, 使用当天的聊天消息计算会话轮次, 按照会话聚合, 统计会话轮次
            SELECT
                day,
                platform,
                shop_id,
                snick,
                cnick,
                real_buyer_nick,
                arraySort(groupArray(msg_milli_timestamp)) AS msg_milli_timestamps,
                arraySort((x, y)->y, groupArray(act), groupArray(msg_milli_timestamp)) AS msg_acts,

                -- 切分会话生成QA切分标记, PS: 可能存在单个Q, 单个A, 单个QA, 多个QA四种情况, 此切分方法只能切分多QA的情况
                arrayMap(
                    (x, y)->(if(x = 'send_msg' AND msg_acts[y-1] = 'recv_msg', 1, 0)),
                    msg_acts,
                    arrayEnumerate(msg_acts)
                ) AS _qa_split_tags,
                -- QA数量
                arraySum(_qa_split_tags) AS dialog_qa_cnt
            FROM (
                SELECT
                    day,
                    platform,
                    shop_id,
                    replaceOne(snick,'cntaobao','') AS snick,
                    replaceOne(cnick,'cntaobao','') AS cnick,
                    '' AS real_buyer_nick,
                    toUInt64(toFloat64(toDateTime64(create_time, 3))*1000) AS msg_milli_timestamp,
                    act
                FROM ods.xdrs_logs_all
                WHERE day = 20230330
                AND shop_id IN ['60b72d421edc070017428380']
                AND act IN ['send_msg', 'recv_msg']
            ) AS xdrs_logs
            GROUP BY day,
                platform,
                shop_id,
                snick,
                cnick,
                real_buyer_nick
        ) AS dialog_info
        LEFT JOIN (
            SELECT
                cnick,
                cnick_id
            FROM dwd.voc_cnick_list_all
            WHERE day = 20230330
            AND platform = 'tb'
        ) AS cnick_info
        USING(cnick)
    ) AS dialog_detail_info
    USING(day, platform, shop_id, snick, cnick, real_buyer_nick)
) AS xdrs_dialog_info
LEFT JOIN (
    -- stage_4, 关联买家最新订单表, 获取订单状态
    SELECT
        day,
        buyer_nick AS cnick,
        order_id,
        order_status_timestamps,
        order_statuses
    FROM dwd.voc_buyer_latest_order_all
    WHERE day = 20230330
    AND shop_id IN ['60b72d421edc070017428380']
) AS latest_order_info
USING(day, cnick)