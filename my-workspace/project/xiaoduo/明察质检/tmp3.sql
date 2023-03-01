select day,
    shop_id,
    'jd' as platform,
    buyer_nick,
    real_buyer_nick,
    snick,
    session_id,
    session_start_time,
    session_end_time,
    is_start_by_cnick,
    is_end_by_cnick,
    focus_goods_ids,
    c_active_send_goods_ids,
    s_active_send_goods_ids,
    order_id,
    goods_id,
    created_time,
    deposited_time as paid_time,
    order_payment,
    goods_payment,
    step_trade_status,
    step_paid_fee,
    order_type,
    goods_num,
    is_refund,
    0 as is_transf,
    cycle,
    1
FROM ft_dwd.persell_ask_order_cov_detail_all
where `day` = toYYYYMMDD(subtractDays(toDate('2023-02-26'), 2 - 1))
    AND shop_id = '5edfa47c8f591c00163ef7d6'
    and `cycle` = 2
    and order_id != ''