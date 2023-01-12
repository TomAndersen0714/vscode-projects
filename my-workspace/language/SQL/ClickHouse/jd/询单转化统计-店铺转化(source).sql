-- ClickHouse
-- insert into pub_app_mp.shop_ask_order_cov_all
SELECT shop_id,
    shop_name,
    ask_type,
    ask_uv,
    create_uv,
    paid_uv,
    order_cnt,
    payment_value,
    paid_payment_rate,
    {{ day }} AS day
FROM (
        SELECT shop_id,
            shop_name,
            ask_type,
            ask_uv,
            create_uv
        FROM (
                SELECT shop_id,
                    shop_name,
                    'ALL' AS ask_type,
                    uniqExact(buyer_name) AS ask_uv
                FROM etl_tmp.ask_order_buyers_all
                WHERE day = {{ day }}
                group by shop_id,
                    shop_name
            ) AS ask_info
            left join (
                SELECT shop_id,
                    uniqExact(buyer_name) AS create_uv
                FROM pub_dws.ask_order_detail_all
                WHERE status = 'created'
                    and send_msg_pv != 0
                    and day = {{ day }}
                group by shop_id
            ) AS create_info using(shop_id)
    ) AS ask_all
    left join (
        SELECT shop_id,
            uniqExact(buyer_name) AS paid_uv,
            uniqExact(order_id) AS order_cnt,
            sum(payment) AS payment_value,
            payment_value / paid_uv AS paid_payment_rate
        FROM pub_dws.ask_order_detail_all
        WHERE status = 'paid'
            and day = {{{ day }}}
        group by shop_id
    ) AS paid_info using(shop_id)


-- ClickHouse
-- 单店铺询单转化计算(TYPE=ALL)
SELECT shop_id,
    shop_name,
    ask_type,
    ask_uv,
    create_uv,
    paid_uv,
    order_cnt,
    payment_value,
    paid_payment_rate,
    toInt32(replace('{{ day }}','-','')) AS day
FROM (
        SELECT shop_id,
            shop_name,
            ask_type,
            ask_uv,
            create_uv
        FROM (
                SELECT
                    shop_id,
                    shop_name,
                    'ALL' AS ask_type,
                    uniqExact(buyer_name) AS ask_uv
                FROM etl_tmp.ask_order_buyers_all
                WHERE day = toInt32(replace('{{ day }}','-',''))
                AND shop_id = '{{ shop_id }}'
                GROUP BY shop_id, shop_name
            ) AS ask_info
            left join (
                SELECT
                    shop_id,
                    uniqExact(buyer_name) AS create_uv
                FROM pub_dws.ask_order_detail_all
                WHERE
                    status = 'created'
                    and send_msg_pv != 0
                    and day = toInt32(replace('{{ day }}','-',''))
                AND shop_id = '{{ shop_id }}'
                GROUP BY shop_id
            ) AS create_info using(shop_id)
    ) AS ask_all
    left join (
        SELECT
            shop_id,
            uniqExact(buyer_name) AS paid_uv,
            uniqExact(order_id) AS order_cnt,
            sum(payment) AS payment_value,
            payment_value / paid_uv AS paid_payment_rate
        FROM pub_dws.ask_order_detail_all
        WHERE status = 'paid'
            and day = toInt32(replace('{{ day }}','-',''))
        AND shop_id = '{{ shop_id }}'
        GROUP BY shop_id
    ) AS paid_info using(shop_id)

-- ClickHouse(etl_tmp.ask_order_buyers_all)
-- insert into etl_tmp.ask_order_buyers_all
select shop_id,
    shop_name,
    snick,
    buyer_name,
    send_msg_from_array,
    act_array,
    auto_reply_pv,
    click_reply_pv,
    human_reply_pv,
    reply_pv,
    { ds_nodash } as day
from (
        select shop_id,
            shop_name,
            snick,
            buyer_name,
            groupArray(send_msg_from) as send_msg_from_array,
            groupArray(act) as act_array,
            countEqual(send_msg_from_array, '0') as auto_reply_pv,
            countEqual(send_msg_from_array, '3') as click_reply_pv,
            countEqual(send_msg_from_array, '2') as human_reply_pv,
            countEqual(act_array, 'send_msg') as reply_pv
        from(
                select shop_id,
                    plat_shop_name as shop_name,
                    snick,
                    buyer_name,
                    send_msg_from,
                    act
                from (
                        select _id as shop_id,
                            plat_shop_name
                        from dim.shop_nick_all
                    ) as shop_info
                    left join (
                        select shop_id,
                            replaceAll(splitByChar(':', snick) [1], 'cnjd', '') AS shop_name,
                            replaceAll(snick, 'cnjd', '') AS snick,
                            replaceAll(cnick, 'cnjd', '') AS buyer_name,
                            if(act = 'recv_msg', '-1', send_msg_from) as send_msg_from,
                            act
                        from pub_dwd.chat_all
                        where day between toYYYYMMDD(
                                subtractDays(parseDateTimeBestEffort(toString({ ds_nodash })),1)
                            ) and { ds_nodash }
                            and abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                        order by create_time asc
                    ) as chat_info using(shop_id)
            ) as chat_info
        group by shop_id,
            shop_name,
            snick,
            buyer_name
    ) as chat
    left join (
        select shop_id,
            buyer_name,
            count(1) as buy_cnt
        from etl_tmp.paid_buyers_all
        where day between toYYYYMMDD(
                subtractDays(
                    parseDateTimeBestEffort(toString({ ds_nodash })),
                    22
                )
            ) and toYYYYMMDD(
                subtractDays(
                    parseDateTimeBestEffort(toString({ ds_nodash })),
                    2
                )
            )
            and abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
        group by shop_id,
            buyer_name
        having buy_cnt = 1
    ) as service 
    on chat.buyer_name = service.buyer_name
where service.buyer_name = ''


-- ClickHouse
select shop_id,
    replaceAll(splitByChar(':', snick) [1], 'cnjd', '') AS shop_name,
    replaceAll(snick, 'cnjd', '') AS snick,
    replaceAll(cnick, 'cnjd', '') AS buyer_name,
    if(act = 'recv_msg', '-1', send_msg_from) as send_msg_from,
    act
from pub_dwd.chat_all
where day between toYYYYMMDD(
        subtractDays(parseDateTimeBestEffort(toString({ ds_nodash })),1)
    ) and { ds_nodash }
    and abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
order by create_time asc