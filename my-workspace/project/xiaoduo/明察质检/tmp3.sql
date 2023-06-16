insert into etl_tmp.ask_order_buyers_all
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
    20230613 as day
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
                            plat_shop_id as plat_shop_name
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
                        where day = 20230613
                            and abs(xxHash64(shop_id)) %16 = 2
                        order by create_time asc
                    ) as chat_info using(shop_id)
            ) as chat_info
        group by shop_id,
            shop_name,
            snick,
            buyer_name
        having has(act_array, 'recv_msg')
    ) as chat
    left join (
        select shop_id,
            buyer_name,
            buy_cnt
        from etl_tmp.paid_buyers_cnt_all
        where abs(xxHash64(shop_id)) %16 = 2
    ) as service on chat.buyer_name = service.buyer_name
where service.buyer_name = ''