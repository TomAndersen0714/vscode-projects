t1. 先定位买家, 起始时间, 终止时间:
    定位3月份买了福袋的买家, 以及首次购买福袋的时间, 以及订单ID, 以及下单福袋后首次下单的时间和订单ID, 生成T1

    SELECT
        time,
        order_id,
        buyer_nick,
        real_buyer_nick,
        plat_goods_ids,
        payment
    FROM ods.order_event_all
    WHERE day BETWEEN 20230301 AND 20230331
    AND shop_id = '5a48c5c489bc46387361988d'
    ORDER BY
        buyer_nick, real_buyer_nick, time ASC

    SELECT
        groupArray(time) AS _times,
        arraySort(_times) AS times,
        arraySort((x,y)->y, groupArray(order_id), _times) AS order_ids,
        arraySort((x,y)->y, groupArray(payment), _times) AS payments,
        arraySort((x,y)->y, groupArray(plat_goods_ids), _times) AS plat_goods_ids_s,

        arrayFilter(
            (x,y)->(hasAny(y, ['681460777534','718092987553']))
            times,
            plat_goods_ids_s
        ) AS is_targets,
        arrayFilter(
            (x,y,z)->arraySum(),
            times,
            arrayCumSum(is_targets) AS is_targets_sum,
            arrayEnumerate(is_targets) AS is_targets_num
        )
    FROM T1
    GROUP BY
        buyer_nick, real_buyer_nick

t2. 查询每天的聊天记录, 导出姓名在T1中, 并且时间在T1对应的起止时间范围内的数据
