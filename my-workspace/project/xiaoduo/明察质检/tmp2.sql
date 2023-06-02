SELECT *
FROM (
    SELECT
        shop_id, snick, cnick, 
        msg_time, msg,
        IF(msg='send_msg', concat(snick, ':\n', msg), concat(cnick, ':\n', msg)) AS content
    FROM ods.xdrs_logs_all
    -- 筛选指定时间范围(强制限制近3天)
    WHERE day BETWEEN toYYYYMMDD(toDate('{{end_date}}') - 3)
        AND toYYYYMMDD(toDate('{{end_date}}'))
    -- 筛选普通消息
    AND act IN ['send_msg', 'recv_msg']
    -- 筛选指定店铺
    AND shop_id GLOBAL IN (
        SELECT
            shop_id
        FROM xqc_dim.xqc_shop_all
        WHERE day = toYYYYMMDD(yesterday())
        -- 筛选指定平台
        AND platform = '{{platform}}'
        -- 筛选指定店铺-主账号
        AND seller_nick = '{{seller_nick}}'
    )
    -- 筛选流失客户
    AND cnick GLOBAL IN (
        -- 筛选x天内, 发生过y次会话, 未下单, 买家信息
        SELECT
            concat('cntaobao', cnick) AS plat_cnick
        FROM (
            SELECT
                platform,
                seller_nick,
                cnick,
                COUNT(DISTINCT _id) AS dialog_cnt
            FROM dwd.xdqc_dialog_all
                -- 筛选指定时间范围(强制限制近3天)
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{end_date}}') -3)
                AND toYYYYMMDD(toDate('{{end_date}}'))
                -- 筛选指定平台
                AND platform = '{{platform}}'
                -- 筛选指定店铺-主账号
                AND seller_nick = '{{seller_nick}}'
                -- 筛选最近未下单买家, 即剔除已下单买家
                AND cnick GLOBAL NOT IN (
                    SELECT DISTINCT
                        buyer_nick
                    FROM ods.order_event_all
                        -- 筛选指定时间范围
                    WHERE day BETWEEN toYYYYMMDD(toDate('{{end_date}}') -2)
                        AND toYYYYMMDD(toDate('{{end_date}}'))
                        -- 筛选指定店铺
                        AND shop_id GLOBAL IN (
                            SELECT
                                shop_id
                            FROM xqc_dim.xqc_shop_all
                            WHERE day = toYYYYMMDD(yesterday())
                            -- 筛选指定平台
                            AND platform = '{{platform}}'
                            -- 筛选指定店铺-主账号
                            AND seller_nick = '{{seller_nick}}'
                        )
                )
            GROUP BY
                platform, seller_nick, cnick
            HAVING
                -- 筛选会话数量
                dialog_cnt > toUInt32('{{dialog_cnt}}')
        ) AS cnick_info
        -- 限制买家数量
        LIMIT 50
    )
    ORDER BY shop_id, snick, cnick, msg_time
)
GROUP BY shop_id, snick, cnick