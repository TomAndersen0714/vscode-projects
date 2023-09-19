SELECT
    day, shop_id, snick, cnick,
    arrayStringConcat(groupArray(content),'\n') AS contents
FROM (
    SELECT
        day,
        shop_id, snick, cnick, 
        msg_time, msg,
        toString(IF(act='send_msg', concat(snick, ' ', toString(msg_time), ':\n', msg), concat(cnick, ' ', toString(msg_time), ':\n', msg))) AS content
    FROM ods.xdrs_logs_all
    -- 筛选指定时间范围(强制限制近3天)
    WHERE day BETWEEN toYYYYMMDD(toDate('{{end_date}}') - 3)
        AND toYYYYMMDD(toDate('{{end_date}}'))
    -- 筛选普通消息
    AND act IN ['send_msg', 'recv_msg']
    
    --筛选买家消息里面包含小二的话术
    WHERE recv_msg LIKE '%小二%'

    -- 筛选指定店铺
    AND shop_id = '{{shop_id}}'
    -- 筛选流失客户
    AND cnick GLOBAL IN (
        -- 筛选x天内, 发生过y次会话, 未下单, 买家信息
        SELECT DISTINCT
            concat('cntaobao', cnick) AS plat_cnick
        FROM (
            SELECT
                platform,
                real_buyer_nick,
                cnick,
                COUNT(DISTINCT _id) AS dialog_cnt
            FROM dwd.xdqc_dialog_all
                -- 筛选指定时间范围(强制限制近3天)
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{end_date}}') -3)
                AND toYYYYMMDD(toDate('{{end_date}}'))
                -- 筛选会话轮次
                AND qa_round_sum > toUInt32('{{会话轮次}}')
                -- 筛选指定平台
                AND platform = '{{platform}}'
                -- 筛选指定店铺-主账号
                AND seller_nick = '{{seller_nick}}'
                -- 筛选最近未下单买家, 即剔除已下单买家
                AND length(order_info_status) = 0
            GROUP BY
                platform, seller_nick, real_buyer_nick, cnick
            HAVING
                -- 筛选会话数量
                dialog_cnt > toUInt32('{{dialog_cnt}}')
        ) AS cnick_info
        -- 限制买家数量
        LIMIT 20
    )
    -- 在这里添加限制snick包含"服务助手"的条件
    -- AND snick LIKE '%服务助手%'
    ORDER BY day, shop_id, snick, cnick, msg_time
)
GROUP BY day, shop_id, snick, cnick