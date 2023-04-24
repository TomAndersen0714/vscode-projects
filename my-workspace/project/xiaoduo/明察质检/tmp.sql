-- 询单周期内所有的会话记录
SELECT
    day,
    shop_id,
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
    s_active_send_goods_ids
FROM tmp.session_filter_all
WHERE
    day BETWEEN toYYYYMMDD(subtractDays(toDate('{{ ds }}'), {{ cycle }} - 1)) AND toYYYYMMDD(toDate('{{ ds }}'))
    AND shop_id = '{{ shop_id }}'
    AND cycle = {{ cycle }}
    AND concat(shop_id, '-', buyer_nick) IN -- 只看询单周期内有订单记录的数据
    (
        SELECT DISTINCT concat(shop_id, '-', buyer_nick)
        FROM ft_dwd.order_detail_all
        WHERE
            day BETWEEN toYYYYMMDD(subtractDays(toDate('{{ ds }}'), {{ cycle }} - 1)) AND toYYYYMMDD(toDate('{{ ds }}'))
            AND shop_id = '{{ shop_id }}'
            AND status IN (
                'created',
                'paid'
            )
            AND order_type != 'step'
    )
