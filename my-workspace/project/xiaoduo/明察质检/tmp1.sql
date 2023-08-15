SELECT
    day, shop_id, snick, cnick,
    arrayStringConcat(groupArray(content),'\n') AS contents
FROM (
    SELECT * 
    FROM (
        SELECT
            day,
            shop_id, snick, cnick, 
            msg_time, msg,
            toString(IF(act='send_msg', concat(snick, ' ', toString(msg_time), ':\n', msg), concat(cnick, ' ', toString(msg_time), ':\n', msg))) AS content
        FROM ods.xdrs_logs_all
        -- 筛选指定时间范围(强制限制近7天)
        PREWHERE day BETWEEN toYYYYMMDD(toDate('{{end_date}}') - 3)
            AND toYYYYMMDD(toDate('{{end_date}}'))
        -- 筛选普通消息
        AND act IN ['send_msg', 'recv_msg']
        -- 筛选指定店铺
        AND shop_id GLOBAL IN (
            SELECT shop_id
            FROM xqc_dim.shop_latest_all
            WHERE company_id = '{{company_id}}'
            AND platform = '{{platform}}'
        )
        -- 筛选买家和客服账号
        AND (snick, cnick) GLOBAL IN (
            -- 筛选包含指定标签的
            SELECT
                concat('cntaobao', snick) AS plat_snick,
                concat('cntaobao', cnick) AS plat_cnick
            FROM (
                SELECT
                    snick,
                    cnick
                FROM dwd.xdqc_dialog_all
                    -- 筛选指定时间范围(强制限制近7天)
                WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{end_date}}') -3)
                    AND toYYYYMMDD(toDate('{{end_date}}'))
                    -- 筛选"顾客提及投诉和举报"打标次数>0的会话
                    AND abnormals_count[22] > 0
                    -- 筛选指定店铺主账号
                    AND seller_nick GLOBAL IN (
                        SELECT seller_nick
                        FROM xqc_dim.shop_latest_all
                        WHERE company_id = '{{company_id}}'
                        AND platform = '{{platform}}'
                    )
                LIMIT 100
            ) AS cnick_info
            -- 限制会话数量
            LIMIT 100
        )
    )
    ORDER BY day, shop_id, snick, cnick, msg_time
)
GROUP BY day, shop_id, snick, cnick