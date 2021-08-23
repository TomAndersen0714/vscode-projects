insert into pub_app.shop_overview_all
select 'tb' as platform,
    shop_data1.shop_id,
    if(served_pv is null, 0, served_pv) as served_pv,
    if(served_uv is null, 0, served_uv) as served_uv,
    if(received_pv is null, 0, received_pv) as received_pv,
    if(received_uv is null, 0, received_uv) as received_uv,
    if(created_uv is null, 0, created_uv) as created_uv,
    if(created_order_cnt is null, 0, created_order_cnt) as created_order_cnt,
    if(created_payment is null, 0, created_payment) as created_payment,
    if(paid_uv is null, 0, paid_uv) as paid_uv,
    if(paid_order_cnt is null, 0, paid_order_cnt) as paid_order_cnt,
    if(paid_payment is null, 0, paid_payment) as paid_payment,
    if(refund_uv is null, 0, refund_uv) as refund_uv,
    if(refund_order_cnt is null, 0, refund_order_cnt) as refund_order_cnt,
    if(refund_order_num is null, 0, refund_order_num) as refund_order_num,
    if(refund_payment is null, 0, refund_payment) as refund_payment,
    {day_int} as day
from (
        select 'tb' as platform,
            shop_recv.shop_id,
            served_uv,
            served_pv,
            received_uv,
            received_pv,
            created_uv,
            created_order_cnt,
            created_payment,
            paid_uv,
            paid_order_cnt,
            paid_payment
        from (
                select shop.shop_id as shop_id,
                    served_uv,
                    served_pv,
                    received_uv,
                    received_pv
                from (
                        SELECT *
                        FROM dim.shop_nick_all
                        where xxHash64(shop_id)%{batch} == {i} -- 按照shop_id分批处理
                    ) as shop
                    left join (
                        select shop_id,
                            count(distinct if(act = 'send_msg', cnick, NULL)) as served_uv,
                            sum(if(act = 'send_msg', 1, 0)) as served_pv,
                            count(distinct if(act = 'recv_msg', cnick, NULL)) as received_uv,
                            sum(if(act = 'recv_msg', 1, 0)) as received_pv
                        from ods.xdrs_logs_all
                        where day = {day_int}
                        and xxHash64(shop_id)%{batch} == {i} -- 按照shop_id分批处理
                        group by shop_id
                    ) as shop_info using(shop_id)
            ) as shop_recv
            left join (
                select shop_id,
                    count(distinct if(status = 'created', buyer_nick, null)) as created_uv,
                    count(distinct if(status = 'created', order_id, null)) as created_order_cnt,
                    sum(if(status = 'created', payment, 0)) as created_payment,
                    count(distinct if(status = 'paid', buyer_nick, null)) as paid_uv,
                    count(distinct if(status = 'paid', order_id, null)) as paid_order_cnt,
                    sum(if(status = 'paid', payment, 0)) as paid_payment
                from ods.order_event_all
                where day = {day_int}
                and xxHash64(shop_id)%{batch} == {i} -- 按照shop_id分批处理
                group by shop_id
            ) as shop_order using(shop_id)
    ) as shop_data1
    left join (
        select shop_id,
            count(distinct buyer_nick) as refund_uv,
            count(distinct oid) as refund_order_cnt,
            sum(num) as refund_order_num,
            sum(payment) as refund_payment
        from (
                select shop_id,
                    plat_shop_name as shop_name
                from dim.shop_nick_all
                where xxHash64(shop_id)%{batch} == {i} -- 按照shop_id分批处理
            ) shop
            left join (
                select seller_nick as shop_name,
                    oid,
                    buyer_nick,
                    payment,
                    num,
                    row_number
                from (
                        select seller_nick,
                            oid,
                            buyer_nick,
                            groupArray(payment) as payment_arr,
                            groupArray(num) as num_arr,
                            groupArray(modified) as modified_arr,
                            arrayEnumerate(modified_arr) as row_number
                        from (
                                select seller_nick,
                                    oid,
                                    buyer_nick,
                                    toFloat64(if(payment = '', '0', payment)) as payment,
                                    toInt64(if(num = '', '0', num)) as num,
                                    modified
                                from ods.reminder_refund_tb_info_all
                                where day = {day_int}
                                    and status = 'WAIT_BUYER_RETURN_GOODS'
                                order by modified desc
                            )
                        group by seller_nick,
                            oid,
                            buyer_nick
                    ) as row_table 
                    array join payment_arr as payment,
                    num_arr as num,
                    row_number as row_number
                where row_number = 1
            ) as refund_info using(shop_name)
        group by shop_id
    ) as shop_data2 using(shop_id)