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
                u_day AS day,
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
                -- stage-1: 查询当天的聊天消息记录
                SELECT
                    toUInt32(day) AS u_day,
                    platform,
                    shop_id,
                    replaceOne(snick,'cntaobao','') AS snick,
                    replaceOne(cnick,'cntaobao','') AS cnick,
                    real_buyer_nick,
                    toUInt64(msg_time) AS msg_timestamp,
                    msg_id,
                    msg,
                    act,
                    send_msg_from,
                    question_b_qid,
                    plat_goods_id
                FROM ods.xdrs_logs_all
                PREWHERE day = {ds_nodash}
                AND shop_id GLOBAL IN (
                    SELECT shop_id
                    FROM xqc_dim.shop_latest_all
                    WHERE company_id GLOBAL IN (
                        SELECT _id
                        FROM xqc_dim.company_latest_all
                        WHERE has(white_list, '{FEATURE_ID}')
                    )
                    AND platform IN {PLATFORMS}
                )
                AND act IN ['send_msg', 'recv_msg']
            ) AS xdrs_logs
            LEFT JOIN (
                SELECT
                    u_day,
                    platform,
                    shop_id,
                    snick,
                    cnick,
                    cnick_info.cnick_id,
                    real_buyer_nick,
                    -- QA数量
                    arraySum(qa_split_tags) AS dialog_qa_cnt
                FROM (
                    -- stage-2: 基于当天的聊天消息计算会话轮次, 按照会话聚合, 统计会话QA次数
                    SELECT
                        u_day,
                        platform,
                        shop_id,
                        snick,
                        cnick,
                        real_buyer_nick,
                        -- 切分会话生成QA切分标记, PS: 可能存在单个Q, 单个A, 单个QA, 多个QA四种情况, 此切分方法只能切分多QA的情况
                        arrayMap(
                            (x, y)->(if(x = 'send_msg' AND sorted_msg_acts[y-1] = 'recv_msg', 1, 0)),
                            sorted_msg_acts,
                            arrayEnumerate(sorted_msg_acts)
                        ) AS qa_split_tags
                    FROM (
                        SELECT
                            u_day,
                            platform,
                            shop_id,
                            snick,
                            cnick,
                            real_buyer_nick,
                            arraySort(groupArray(msg_milli_timestamp)) AS msg_milli_timestamps,
                            arraySort((x, y)->y, groupArray(act), groupArray(msg_milli_timestamp)) AS sorted_msg_acts
                        FROM (
                            SELECT
                                toUInt32(day) AS u_day,
                                platform,
                                shop_id,
                                replaceOne(snick,'cntaobao','') AS snick,
                                replaceOne(cnick,'cntaobao','') AS cnick,
                                real_buyer_nick,
                                toUInt64(toFloat64(toDateTime64(create_time, 3))*1000) AS msg_milli_timestamp,
                                act
                            FROM ods.xdrs_logs_all
                            PREWHERE day = {ds_nodash}
                            AND shop_id GLOBAL IN (
                                SELECT shop_id
                                FROM xqc_dim.shop_latest_all
                                WHERE company_id GLOBAL IN (
                                    SELECT _id
                                    FROM xqc_dim.company_latest_all
                                    WHERE has(white_list, '{FEATURE_ID}')
                                )
                                AND platform IN {PLATFORMS}
                            )
                            AND act IN ['send_msg', 'recv_msg']
                        )
                        GROUP BY u_day,
                            platform,
                            shop_id,
                            snick,
                            cnick,
                            real_buyer_nick
                        -- 限制单次会话数据量, 避免脏数据
                        HAVING count(1) < 2000
                    ) AS xdrs_logs
                ) AS dialog_info
                LEFT JOIN (
                    SELECT
                        cnick,
                        cnick_id
                    FROM dwd.voc_cnick_list_all
                    WHERE day = {ds_nodash}
                    -- 筛选当日咨询客户
                    AND (platform, cnick) IN (
                        SELECT DISTINCT
                            platform,
                            replaceOne(cnick,'cntaobao','') AS cnick
                        FROM ods.xdrs_logs_all
                        PREWHERE day = {ds_nodash}
                        AND shop_id GLOBAL IN (
                            SELECT shop_id
                            FROM xqc_dim.shop_latest_all
                            WHERE company_id GLOBAL IN (
                                SELECT _id
                                FROM xqc_dim.company_latest_all
                                WHERE has(white_list, '{FEATURE_ID}')
                            )
                            AND platform IN {PLATFORMS}
                        )
                        AND act IN ['send_msg', 'recv_msg']
                    )
                ) AS cnick_info
                USING(cnick)
            ) AS dialog_detail_info
            USING(u_day, platform, shop_id, snick, cnick, real_buyer_nick)
        ) AS xdrs_dialog_info
        LEFT JOIN (
            -- stage-3: 关联买家最新订单表, 查询订单状态
            SELECT
                day,
                platform,
                shop_id,
                buyer_nick AS cnick,
                order_id,
                order_status_timestamps,
                order_statuses
            FROM dwd.voc_buyer_latest_order_all
            WHERE day = {ds_nodash}
            AND shop_id GLOBAL IN (
                SELECT shop_id
                FROM xqc_dim.shop_latest_all
                WHERE company_id GLOBAL IN (
                    SELECT _id
                    FROM xqc_dim.company_latest_all
                    WHERE has(white_list, '{FEATURE_ID}')
                )
                AND platform IN {PLATFORMS}
            )
            -- 筛选当日咨询客户
            AND (platform, buyer_nick) IN (
                SELECT DISTINCT
                    platform,
                    replaceOne(cnick,'cntaobao','') AS cnick
                FROM ods.xdrs_logs_all
                PREWHERE day = {ds_nodash}
                AND shop_id GLOBAL IN (
                    SELECT shop_id
                    FROM xqc_dim.shop_latest_all
                    WHERE company_id GLOBAL IN (
                        SELECT _id
                        FROM xqc_dim.company_latest_all
                        WHERE has(white_list, '{FEATURE_ID}')
                    )
                    AND platform IN {PLATFORMS}
                )
                AND act IN ['send_msg', 'recv_msg']
            )
        ) AS latest_order_info
        USING(day, platform, shop_id, cnick)