SELECT platform,
    task_id,
    xdrs_cnick AS buyer_nick,
    shop_id,
    send_channel,
    sub_nick,
    send_time,
    status,
    has_reply,
    has_order,
    first_order_time,
    order_amount,
    order_ids,
    pay_amount,
    pay_orders
FROM (
        SELECT *
        FROM (
                SELECT y1.*,
                    if(x3.buyer_nick = '', 0, 1) AS has_order,
                    first_order_time,
                    order_amount,
                    order_ids
                FROM (
                        SELECT x1.*,
                            if(x2.buyer_nick = '', 0, 1) AS has_reply
                        FROM (
                                SELECT 'ks' AS platform,
                                    task_id,
                                    cnick AS xdrs_cnick,
                                    platform_cnick AS buyer_nick,
                                    shop_id,
                                    'ks' AS send_channel,
                                    snick AS sub_nick,
                                    toString(create_time) AS send_time,
                                    'succ' AS status
                                FROM ods.xdrs_log_all
                                WHERE day between 20230807 and 20230812
                                    AND shop_id = '61824ba1951c610010ce6cbc'
                                    AND task_id = '64d094e394648322a056beb6'
                                    AND act = 'send_fishpond_msg'
                                UNION ALL
                                SELECT *
                                FROM (
                                        SELECT platform,
                                            task_id,
                                            buyer_nick as xdrs_cnick,
                                            buyer_nick,
                                            shop_id,
                                            send_channel,
                                            b.plat_user_id AS sub_nick,
                                            toString(send_time) AS send_time,
                                            sned_status
                                        FROM (
                                                SELECT platform,
                                                    task_id,
                                                    if(nick = '', phone, nick) AS buyer_nick,
                                                    shop_id,
                                                    'sms' AS send_channel,
                                                    toDateTime64(min(send_ts), 3) AS send_time,
                                                    'succ' AS sned_status
                                                FROM ods.sms_feedback_all
                                                WHERE day BETWEEN 20230807 AND 20230812
                                                    AND shop_id = '61824ba1951c610010ce6cbc'
                                                    AND task_id = '64d094e394648322a056beb6'
                                                    AND platform = 'ks'
                                                    AND service = 'fishpond'
                                                    AND status = 1
                                                group by platform,
                                                    task_id,
                                                    buyer_nick,
                                                    shop_id
                                            ) AS a
                                            LEFT JOIN dim.shop AS b ON a.shop_id = b._id
                                    ) AS sms_message
                            ) x1
                            LEFT JOIN (
                                SELECT DISTINCT cnick AS buyer_nick
                                FROM (
                                        SELECT platform_cnick AS cnick,
                                            task_id,
                                            create_time
                                        FROM ods.xdrs_log_all
                                        WHERE day between 20230807 and 20230812
                                            AND shop_id = '61824ba1951c610010ce6cbc'
                                            AND task_id = '64d094e394648322a056beb6'
                                            AND act = 'send_fishpond_msg'
                                    ) t1
                                    JOIN (
                                        SELECT platform_cnick AS cnick,
                                            create_time
                                        FROM ods.xdrs_log_all
                                        WHERE day between 20230807 and 20230812
                                            AND shop_id = '61824ba1951c610010ce6cbc'
                                            AND act = 'recv_msg'
                                    ) t2 USING(cnick)
                                WHERE t1.create_time <= t2.create_time
                                    AND dateDiff(
                                        'second',
                                        toDateTime(substring(toString(t2.create_time), 1, 19)),
                                        toDateTime(substring(toString(t1.create_time), 1, 19))
                                    ) <= 432000
                            ) x2 USING (buyer_nick)
                    ) y1
                    LEFT JOIN (
                        SELECT cnick AS buyer_nick,
                            sum(payment) AS order_amount,
                            min(`time`) AS first_order_time,
                            groupArray(order_id) AS order_ids
                        FROM (
                                SELECT t1.cnick AS cnick,
                                    if(
                                        t1.payment = 0,
                                        if(t2.payment IS NULL, 0, t2.payment),
                                        t1.payment
                                    ) AS payment,
                                    order_id,
                                    t1.time AS time
                                FROM (
                                        SELECT platform_cnick AS cnick,
                                            toFloat64(max(payment)) AS payment,
                                            order_id,
                                            min(time) as time
                                        FROM ods.fishpond_conversion_all
                                        WHERE status = 'created'
                                            AND day between 20230807 and 20230812
                                            AND shop_id = '61824ba1951c610010ce6cbc'
                                            AND task_id = '64d094e394648322a056beb6'
                                        GROUP BY cnick,
                                            order_id
                                    ) AS t1
                                    LEFT JOIN (
                                        SELECT order_id,
                                            toFloat64(max(payment) / 100) AS payment
                                        FROM ods.order_event_all
                                        WHERE day BETWEEN 20230807 AND 20230827
                                            AND shop_id = '61824ba1951c610010ce6cbc'
                                            AND order_id IN (
                                                SELECT DISTINCT order_id
                                                FROM ods.fishpond_conversion_all
                                                WHERE day BETWEEN 20230807 AND 20230812
                                                    AND status = 'created'
                                                    AND payment = 0
                                                    AND shop_id = '61824ba1951c610010ce6cbc'
                                                    AND task_id = '64d094e394648322a056beb6'
                                            )
                                        GROUP BY order_id
                                    ) AS t2 USING(order_id)
                            )
                        GROUP BY buyer_nick
                    ) x3 USING(buyer_nick)
            ) y2
            LEFT JOIN (
                SELECT cnick AS buyer_nick,
                    sum(payment) AS pay_amount,
                    groupArray(order_id) AS pay_orders
                FROM (
                        SELECT t1.cnick,
                            if(
                                t1.payment = 0,
                                if(t2.payment IS NULL, 0, t2.payment),
                                t1.payment
                            ) AS payment,
                            order_id
                        FROM (
                                SELECT platform_cnick AS cnick,
                                    toFloat64(max(payment)) AS payment,
                                    order_id
                                FROM ods.fishpond_conversion_all
                                WHERE day BETWEEN 20230807 AND 20230812
                                    AND status = 'paid'
                                    AND shop_id = '61824ba1951c610010ce6cbc'
                                    AND task_id = '64d094e394648322a056beb6'
                                GROUP BY cnick,
                                    order_id
                            ) AS t1
                            LEFT JOIN (
                                SELECT order_id,
                                    toFloat64(max(payment) / 100) AS payment
                                FROM ods.order_event_all
                                WHERE day BETWEEN 20230807 AND 20230827
                                    AND shop_id = '61824ba1951c610010ce6cbc'
                                    AND order_id IN (
                                        SELECT DISTINCT order_id
                                        FROM ods.fishpond_conversion_all
                                        WHERE day BETWEEN 20230807 AND 20230812
                                            AND status = 'paid'
                                            AND payment = 0
                                            AND shop_id = '61824ba1951c610010ce6cbc'
                                            AND task_id = '64d094e394648322a056beb6'
                                    )
                                GROUP BY order_id
                            ) AS t2 USING(order_id)
                    )
                GROUP BY buyer_nick
            ) x4 USING (buyer_nick)
    )
WHERE 1 = 1
ORDER BY send_time,
    buyer_nick