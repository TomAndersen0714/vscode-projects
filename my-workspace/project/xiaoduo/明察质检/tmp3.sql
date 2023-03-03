insert into etl_tmp.ask_order_detail_all
select buyer_info.*
from (
        select shop_id,
            order_id,
            shop_name,
            groupArray(snick) [1] as snick,
            buyer_nick as buyer_name,
            status,
            toString(order_time) as order_time,
            payment,
            goods_count,
            day
        from(
                select chat_info.day,
                    chat_info.shop_id,
                    chat_info.shop_name,
                    chat_info.snick,
                    chat_info.buyer_nick,
                    mode,
                    act,
                    chat_time,
                    status,
                    order_time,
                    send_msg_from,
                    order_id,
                    payment,
                    goods_count,
                    abs (chat_time - order_time) as time_abs
                from (
                        select day,
                            shop_id,
                            replaceAll(splitByChar(':', snick) [1], 'cnjd', '') as shop_name,
                            replaceAll(snick, 'cnjd', '') as snick,
                            replaceAll(cnick, 'cnjd', '') as buyer_nick,
                            mode,
                            act,
                            if(act = 'recv_msg', '-1', send_msg_from) as send_msg_from,
                            parseDateTimeBestEffortOrNull(toString(create_time)) as chat_time
                        from pub_dwd.chat_all
                        where day = { day }
                            and abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                            and act = 'recv_msg'
                    ) as chat_info
                    left join (
                        select shop_id,
                            order_id,
                            buyer_nick,
                            payment,
                            status,
                            arraySum(goods_count) AS goods_count,
                            parseDateTimeBestEffortOrNull(toString(time)) as order_time
                        from pub_dwd.order_event_all
                        where day = { day }
                            and status in ('created', 'paid')
                            and abs(xxHash64(shop_id)) % { hash_part } = { hast_no }
                    ) as order_info using (shop_id, buyer_nick)
                where status != ''
                order by time_abs asc
            ) as ask_order_info
        where act = 'recv_msg'
        group by day,
            shop_id,
            order_id,
            payment,
            goods_count,
            shop_name,
            buyer_nick,
            order_time,
            status
    ) as buyer_info
    left join (
        select shop_id,
            shop_name,
            snick,
            buyer_name
        from etl_tmp.ask_order_buyers_all
        where day = { day }
    ) as ask_info using(shop_id, snick, buyer_name)
where ask_info.buyer_name != ''



insert into pub_dws.ask_order_detail_all
select a.*
from (
        select chat_info.shop_id as shop_id,
            detail.order_id as order_id,
            chat_info.shop_name as shop_name,
            chat_info.snick as snk_name,
            chat_info.buyer_nick as buyer_name,
            detail.status as status,
            detail.payment as payment,
            detail.goods_count as goods_cnt,
            detail.order_time as order_time,
            groupArray(send_msg_from) as send_msg_from_array,
            if(
                send_msg_from_array [-1] in ('0', '3'),
                'True',
                'False'
            ) as last_is_robot_send,
            if(has(send_msg_from_array, '2') = 1, 'False', 'True') as is_robot_serve,
            sum(if(send_msg_from in ('0', '3'), 1, 0)) as robot_send_msg_pv,
            sum(if(send_msg_from = '2', 1, 0)) as human_send_msg_pv,
            count(1) - countEqual(send_msg_from_array, '-1') send_msg_pv,
            chat_info.day as day
        from (
                select { day } as day,
                    shop_id,
                    replaceAll(splitByChar(':', snick) [1], 'cnjd', '') as shop_name,
                    replaceAll(snick, 'cnjd', '') as snick,
                    replaceAll(cnick, 'cnjd', '') as buyer_nick,
                    mode,
                    if(act = 'recv_msg', '-1', send_msg_from) as send_msg_from,
                    parseDateTimeBestEffortOrNull(toString(create_time)) as chat_time
                from pub_dwd.chat_all
                where day between toYYYYMMDD(
                        subtractDays(
                            parseDateTimeBestEffortOrNull(toString({ day })),
                            1
                        )
                    )
                    and { day }
            ) as chat_info
            left join (
                select shop_id,
                    order_id,
                    shop_name,
                    snick,
                    buyer_nick,
                    status,
                    parseDateTimeBestEffortOrNull(order_time) as order_time,
                    payment,
                    goods_count
                from etl_tmp.ask_order_detail_all
                where day = { day }
            ) as detail using(shop_id, snick, buyer_nick)
        where detail.order_id != ''
        group by day,
            shop_id,
            shop_name,
            status,
            snk_name,
            buyer_name,
            order_id,
            order_time,
            payment,
            goods_count
    ) as a
    left join (
        select distinct shop_id,
            buyer_name
        from etl_tmp.ask_order_buyers_all
        where day = { day }
    ) as b using (shop_id, buyer_name)
where b.buyer_name != ''