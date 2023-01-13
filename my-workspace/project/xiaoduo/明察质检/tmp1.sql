SELECT corp_id,
    shop_num as "店铺数",
    external_user_count as "下单顾客人数",
    bind_nick_count as "下单账号数",
    payment_sum as "支付总额",
    order_count as "订单数",
    per_payment as "客单价",
    new_payment_sum as "首购支付总额",
    new_order_count as "首购订单数",
    new_per_payment as "首购客单价",
    old_payment_sum as "复购支付总额",
    old_order_count as "复购订单数",
    old_per_payment as "复购客单价"
FROM (
    SELECT corp_id,
        count(DISTINCT shop_id) as shop_num,
        count(DISTINCT external_user_id) as external_user_count,
        count(DISTINCT bind_nick) as bind_nick_count,
        arraySum(
            arrayConcat(new_payments, old_payments) as payments
        ) / 100 as payment_sum,
        length(payments) as order_count,
        floor(if(order_count > 0, payment_sum / order_count, 0), 2) as per_payment,
        arraySum(
            arrayFilter(
                (p, o, i)->arrayFirstIndex((x)->(x = o), new_order_ids) = i,
                groupArrayIf(payment, tag = 0),
                groupArrayIf(order_id, tag = 0) as new_order_ids,
                arrayEnumerate(new_order_ids)
            ) as new_payments
        ) / 100 as new_payment_sum,
        length(new_payments) as new_order_count,
        floor(
            if(
                new_order_count > 0,
                new_payment_sum / new_order_count,
                0
            ),
            2
        ) as new_per_payment,
        arraySum(
            arrayFilter(
                (p, o, i)->arrayFirstIndex((x)->(x = o), old_order_ids) = i,
                groupArrayIf(payment, tag = 1),
                groupArrayIf(order_id, tag = 1) as old_order_ids,
                arrayEnumerate(old_order_ids)
            ) as old_payments
        ) / 100 as old_payment_sum,
        length(old_payments) as old_order_count,
        floor(
            if(
                old_order_count > 0,
                old_payment_sum / old_order_count,
                0
            ),
            2
        ) as old_per_payment
    FROM (
            SELECT day,
                corp_id,
                shop_id,
                bind_nick,
                external_user_id,
                tupleElement(arrayJoin(tmps) as tmp, 1) as order_id,
                tupleElement(tmp, 2) as payment,
                tupleElement(tmp, 3) as order_time,
                before.first_order_time,
                before.first_order_time > 0 AND before.first_order_time < order_time as tag
            FROM (
                WITH
                    toYYYYMMDD(toDate('2023-01-05')) AS starts,
                    toYYYYMMDD(toDate('2023-01-11')) AS ends
                SELECT day,
                    platform,
                    corp_id,
                    shop_id,
                    bind_nick,
                    external_user_id,
                    arrayMap(
                        (x, y, z)->tuple(x, y, z),
                        order_ids,
                        payments,
                        order_times
                    ) as tmps
                FROM app_corp.corp_shop_nick_bind_order_stat_2_all
                WHERE day BETWEEN starts AND ends -- AND platform='tb'
                    AND corp_id = 'ww07ca2dcba6ae4bc9'
                    AND length(order_ids) > 0
                    AND bind_nick not like '%*%'
            )
            LEFT JOIN (
                WITH toYYYYMMDD(
                        date_add(
                            DAY,
                            toInt64('-7'),
                            addDays(toDate('2023-01-05'), -1)
                        )
                    ) AS compare_starts,
                    toYYYYMMDD(addDays(toDate('2023-01-05'), -1)) AS compare_ends
                SELECT platform,
                    corp_id,
                    shop_id,
                    bind_nick,
                    arrayMin(groupArrayArray(order_times)) as first_order_time
                FROM app_corp.corp_shop_nick_bind_order_stat_2_all
                WHERE day BETWEEN compare_starts AND compare_ends
                    AND corp_id = 'ww07ca2dcba6ae4bc9' -- AND platform='tb'
                    AND length(order_ids) > 0
                    AND bind_nick not like '%*%'
                GROUP BY platform,
                    corp_id,
                    shop_id,
                    bind_nick
            ) AS before
            using (platform, corp_id, shop_id, bind_nick)
        )
    GROUP BY corp_id
) -- trace:05428cd180aae09b6e2275abd324a495