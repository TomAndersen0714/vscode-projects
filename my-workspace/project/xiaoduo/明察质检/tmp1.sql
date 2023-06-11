SELECT
    buyer_nick, real_buyer_nick,
    target_times,
    target_order_ids,
    target_plat_goods_ids_s,
    target_payments
FROM (
    SELECT
        buyer_nick,
        real_buyer_nick,
        groupArray(time) AS _times,
        arraySort(_times) AS times,
        arraySort((x,y)->y, groupArray(order_id), _times) AS order_ids,
        arraySort((x,y)->y, groupArray(plat_goods_ids), _times) AS plat_goods_ids_s,
        arraySort((x,y)->y, groupArray(payment), _times) AS payments,
        
        arrayMap(
            (x)->(hasAny(x, ['681460777534','718092987553'])),
            plat_goods_ids_s
        ) AS has_goods_s,
        arrayMap(
            (x,y)->(y=1),
            arrayEnumerate(times),
            arrayCumSum(has_goods_s) AS has_goods_sums
        ) AS is_targets,
        arrayFilter(
            (x,y)->(y=1),
            times,
            is_targets
        ) AS target_times,
        arrayFilter(
            (x,y)->(y=1),
            order_ids,
            is_targets
        ) AS target_order_ids,
        arrayFilter(
            (x,y)->(y=1),
            plat_goods_ids_s,
            is_targets
        ) AS target_plat_goods_ids_s,
        arrayFilter(
            (x,y)->(y=1),
            payments,
            is_targets
        ) AS target_payments
        
    FROM (
        SELECT
            buyer_nick,
            real_buyer_nick,
            time,
            order_id,
            plat_goods_ids,
            payment
        FROM ods.order_event_tb_all
        WHERE day BETWEEN 20230301 AND 20230331
        AND shop_id = '5a48c5c489bc46387361988d'
        AND status = 'succeeded'
        ORDER BY
            buyer_nick, real_buyer_nick, time ASC
    )
    GROUP BY
        buyer_nick, real_buyer_nick
    HAVING length(target_times)>1
)