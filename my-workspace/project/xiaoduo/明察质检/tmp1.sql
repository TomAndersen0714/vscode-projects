with c1 as (
    SELECT _id,
        shop_id,
        toDateTime(create_at) AS send_time_start,
        toYYYYMMDD(toDateTime(create_at)) AS `day`
    FROM dim.fishpond_customized_task_all
    WHERE toYYYYMMDD(parseDateTimeBestEffort('{day}')) BETWEEN `day` AND toYYYYMMDD(addDays(send_time_start, 5))
        and abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
    UNION ALL
    SELECT _id,
        shop_id,
        toDateTime(send_time_start) AS send_time_start,
        `day`
    FROM dim.fishpond_task_all
    WHERE toYYYYMMDD(parseDateTimeBestEffort('{day}')) BETWEEN `day` AND toYYYYMMDD(addDays(send_time_start, 5))
        and abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
),
c2 as (
    select distinct shop_id
    from c1
)
SELECT task_id,
    buyer_nick,
    platform,
    shop_id,
    sub_nick,
    send_time,
    has_reply,
    first_order_time,
    order_amount,
    order_ids,
    pay_amount,
    pay_orders,
    send_channel,
    toYYYYMMDD(parseDateTimeBestEffort(send_time)) as `day`
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
                                SELECT 'jd' AS platform,
                                    task_id,
                                    lower(replace(cnick, 'cnjd', '')) AS xdrs_cnick,
                                    --lower转小写    cnick:cnjdjd_6393197d91111
                                    lower(replace(cnick, 'cnjd', '')) AS buyer_nick,
                                    shop_id,
                                    'jd' AS send_channel,
                                    replace(snick, 'cnjd', '') AS sub_nick,
                                    create_time AS send_time,
                                    'succ' AS status
                                FROM ods.xdrs_log_all
                                WHERE day between { day_5 } and { day }
                                    and shop_id in (
                                        select shop_id
                                        from c2
                                    )
                                    and abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                    AND task_id GLOBAL IN (
                                        SELECT DISTINCT _id
                                        FROM (
                                                SELECT _id,
                                                    shop_id,
                                                    toDateTime(create_at) AS send_time_start,
                                                    toYYYYMMDD(toDateTime(create_at)) AS `day`
                                                FROM dim.fishpond_customized_task_all
                                                WHERE toYYYYMMDD(parseDateTimeBestEffort('{day}')) BETWEEN `day` AND toYYYYMMDD(addDays(send_time_start, 5))
                                                    AND abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                                UNION ALL
                                                SELECT _id,
                                                    shop_id,
                                                    toDateTime(send_time_start) AS send_time_start,
                                                    `day`
                                                FROM dim.fishpond_task_all
                                                WHERE toYYYYMMDD(parseDateTimeBestEffort('{day}')) BETWEEN `day` AND toYYYYMMDD(addDays(send_time_start, 5))
                                                    AND abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                            )
                                    )
                                    AND act = 'send_fishpond_msg' --鱼池发送消息
                                UNION ALL
                                SELECT *
                                FROM (
                                        SELECT platform,
                                            task_id,
                                            buyer_nick as xdrs_cnick,
                                            buyer_nick,
                                            shop_id,
                                            send_channel,
                                            b.plat_shop_name AS sub_nick,
                                            toString(send_time) AS send_time,
                                            sned_status
                                        FROM (
                                                SELECT platform,
                                                    task_id,
                                                    if(nick = '', phone, nick) AS buyer_nick,
                                                    shop_id,
                                                    'sms' AS send_channel,
                                                    -- 短信发送
                                                    toDateTime64(min(send_ts), 3) AS send_time,
                                                    'succ' AS sned_status
                                                FROM ods.sms_feedback_all -- 短信表
                                                WHERE day BETWEEN { day_5 } AND { day }
                                                    and abs(xxHash64(shop_id)) % { hash_part } = { hast_no } -- shop_id 使用hash分区
                                                    AND task_id GLOBAL IN (
                                                        SELECT DISTINCT _id
                                                        FROM (
                                                                SELECT _id,
                                                                    shop_id,
                                                                    toDateTime(create_at) AS send_time_start,
                                                                    toYYYYMMDD(toDateTime(create_at)) AS `day`
                                                                FROM dim.fishpond_customized_task_all
                                                                WHERE toYYYYMMDD(parseDateTimeBestEffort('{day}')) BETWEEN `day` AND toYYYYMMDD(addDays(send_time_start, 5))
                                                                    AND abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                                                UNION ALL
                                                                SELECT _id,
                                                                    shop_id,
                                                                    toDateTime(send_time_start) AS send_time_start,
                                                                    `day`
                                                                FROM dim.fishpond_task_all
                                                                WHERE toYYYYMMDD(parseDateTimeBestEffort('{day}')) BETWEEN `day` AND toYYYYMMDD(addDays(send_time_start, 5))
                                                                    AND abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                                            )
                                                    )
                                                    AND platform = 'jd'
                                                    AND service = 'fishpond'
                                                    AND status = 1
                                                group by platform,
                                                    task_id,
                                                    buyer_nick,
                                                    shop_id
                                            ) AS a
                                            LEFT JOIN dim.shop_nick_all AS b ON a.shop_id = b.plat_shop_id
                                    ) AS sms_message
                            ) x1
                            LEFT JOIN (
                                SELECT DISTINCT lower(replace(cnick, 'cnjd', '')) AS buyer_nick
                                FROM (
                                        SELECT cnick AS cnick,
                                            task_id,
                                            create_time
                                        FROM ods.xdrs_log_all
                                        WHERE day between { day_5 } and { day }
                                            and shop_id in (
                                                select shop_id
                                                from c2
                                            )
                                            and abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                            AND task_id GLOBAL IN (
                                                SELECT DISTINCT _id
                                                FROM (
                                                        SELECT _id,
                                                            shop_id,
                                                            toDateTime(create_at) AS send_time_start,
                                                            toYYYYMMDD(toDateTime(create_at)) AS `day`
                                                        FROM dim.fishpond_customized_task_all
                                                        WHERE toYYYYMMDD(parseDateTimeBestEffort('{day}')) BETWEEN `day` AND toYYYYMMDD(addDays(send_time_start, 5))
                                                            AND abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                                        UNION ALL
                                                        SELECT _id,
                                                            shop_id,
                                                            toDateTime(send_time_start) AS send_time_start,
                                                            `day`
                                                        FROM dim.fishpond_task_all
                                                        WHERE toYYYYMMDD(parseDateTimeBestEffort('{day}')) BETWEEN `day` AND toYYYYMMDD(addDays(send_time_start, 5))
                                                            AND abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                                    )
                                            )
                                            AND act = 'send_fishpond_msg'
                                    ) t1
                                    JOIN (
                                        SELECT cnick AS cnick,
                                            create_time
                                        FROM ods.xdrs_log_all
                                        WHERE day between { day_5 } and { day }
                                            and shop_id in (
                                                select shop_id
                                                from c2
                                            )
                                            and abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                            AND act = 'recv_msg'
                                    ) t2 USING(cnick) -- 买家发送的消息
                                WHERE t1.create_time <= t2.create_time --卖家发送消息 要小于等于 买家发送的消息
                                    AND dateDiff(
                                        'second',
                                        toDateTime(substring(t2.create_time, 1, 19)),
                                        toDateTime(substring(t1.create_time, 1, 19))
                                    ) <= 432000 --差值是秒
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
                                        SELECT cnick AS cnick,
                                            toFloat64(max(payment)) AS payment,
                                            order_id,
                                            min(time) as time
                                        FROM ods.fishpond_conversion_all --客伴任务相关订单表
                                        WHERE status = 'created'
                                            AND day between { day_5 } AND { day }
                                            and abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                            AND task_id GLOBAL IN (
                                                SELECT DISTINCT _id
                                                FROM (
                                                        SELECT _id,
                                                            shop_id,
                                                            toDateTime(create_at) AS send_time_start,
                                                            toYYYYMMDD(toDateTime(create_at)) AS `day`
                                                        FROM dim.fishpond_customized_task_all
                                                        WHERE toYYYYMMDD(parseDateTimeBestEffort('{day}')) BETWEEN `day` AND toYYYYMMDD(addDays(send_time_start, 5))
                                                            AND abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                                        UNION ALL
                                                        SELECT _id,
                                                            shop_id,
                                                            toDateTime(send_time_start) AS send_time_start,
                                                            `day`
                                                        FROM dim.fishpond_task_all
                                                        WHERE toYYYYMMDD(parseDateTimeBestEffort('{day}')) BETWEEN `day` AND toYYYYMMDD(addDays(send_time_start, 5))
                                                            AND abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                                    )
                                            )
                                        GROUP BY cnick,
                                            order_id
                                    ) AS t1
                                    LEFT JOIN (
                                        SELECT order_id,
                                            toFloat64(max(payment) / 100) AS payment
                                        FROM ods.order_event_all
                                        WHERE day BETWEEN { day_5 } AND { day }
                                            and shop_id in (
                                                select shop_id
                                                from c2
                                            )
                                            and abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                            AND order_id IN (
                                                SELECT DISTINCT order_id
                                                FROM ods.fishpond_conversion_all
                                                WHERE day BETWEEN { day_5 } AND { day }
                                                    AND status = 'created'
                                                    AND payment = 0
                                                    and abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                                    AND task_id GLOBAL IN (
                                                        SELECT DISTINCT _id
                                                        FROM (
                                                                SELECT _id,
                                                                    shop_id,
                                                                    toDateTime(create_at) AS send_time_start,
                                                                    toYYYYMMDD(toDateTime(create_at)) AS `day`
                                                                FROM dim.fishpond_customized_task_all
                                                                WHERE toYYYYMMDD(parseDateTimeBestEffort('{day}')) BETWEEN `day` AND toYYYYMMDD(addDays(send_time_start, 5))
                                                                    AND abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                                                UNION ALL
                                                                SELECT _id,
                                                                    shop_id,
                                                                    toDateTime(send_time_start) AS send_time_start,
                                                                    `day`
                                                                FROM dim.fishpond_task_all
                                                                WHERE toYYYYMMDD(parseDateTimeBestEffort('{day}')) BETWEEN `day` AND toYYYYMMDD(addDays(send_time_start, 5))
                                                                    AND abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                                            )
                                                    )
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
                    groupArray(order_id) AS pay_orders -- 列转行
                FROM (
                        SELECT t1.cnick,
                            if(
                                t1.payment = 0,
                                if(t2.payment IS NULL, 0, t2.payment),
                                t1.payment
                            ) AS payment,
                            order_id
                        FROM (
                                SELECT cnick AS cnick,
                                    toFloat64(max(payment)) AS payment,
                                    order_id
                                FROM ods.fishpond_conversion_all
                                WHERE day BETWEEN { day_5 } AND { day }
                                    AND status = 'paid'
                                    and abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                    AND task_id GLOBAL IN (
                                        SELECT DISTINCT _id
                                        FROM (
                                                SELECT _id,
                                                    shop_id,
                                                    toDateTime(create_at) AS send_time_start,
                                                    toYYYYMMDD(toDateTime(create_at)) AS `day`
                                                FROM dim.fishpond_customized_task_all
                                                WHERE toYYYYMMDD(parseDateTimeBestEffort('{day}')) BETWEEN `day` AND toYYYYMMDD(addDays(send_time_start, 5))
                                                    AND abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                                UNION ALL
                                                SELECT _id,
                                                    shop_id,
                                                    toDateTime(send_time_start) AS send_time_start,
                                                    `day`
                                                FROM dim.fishpond_task_all
                                                WHERE toYYYYMMDD(parseDateTimeBestEffort('{day}')) BETWEEN `day` AND toYYYYMMDD(addDays(send_time_start, 5))
                                                    AND abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                            )
                                    )
                                GROUP BY cnick,
                                    order_id
                            ) AS t1
                            LEFT JOIN (
                                SELECT order_id,
                                    toFloat64(max(payment) / 100) AS payment
                                FROM ods.order_event_all
                                WHERE day BETWEEN { day_5 } AND { day }
                                    and shop_id in (
                                        select shop_id
                                        from c2
                                    )
                                    and abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                    AND order_id IN (
                                        SELECT DISTINCT order_id
                                        FROM ods.fishpond_conversion_all --客伴任务相关订单表
                                        WHERE day BETWEEN { day_5 } AND { day }
                                            AND status = 'paid'
                                            AND payment = 0
                                            and abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                            AND task_id GLOBAL IN (
                                                SELECT DISTINCT _id
                                                FROM (
                                                        SELECT _id,
                                                            shop_id,
                                                            toDateTime(create_at) AS send_time_start,
                                                            toYYYYMMDD(toDateTime(create_at)) AS `day`
                                                        FROM dim.fishpond_customized_task_all
                                                        WHERE toYYYYMMDD(parseDateTimeBestEffort('{day}')) BETWEEN `day` AND toYYYYMMDD(addDays(send_time_start, 5))
                                                            AND abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                                        UNION ALL
                                                        SELECT _id,
                                                            shop_id,
                                                            toDateTime(send_time_start) AS send_time_start,
                                                            `day`
                                                        FROM dim.fishpond_task_all
                                                        WHERE toYYYYMMDD(parseDateTimeBestEffort('{day}')) BETWEEN `day` AND toYYYYMMDD(addDays(send_time_start, 5))
                                                            AND abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                                                    )
                                            )
                                    )
                                GROUP BY order_id
                            ) AS t2 USING(order_id)
                    )
                GROUP BY buyer_nick
            ) x4 USING (buyer_nick)
    )
ORDER BY send_time,
    buyer_nick