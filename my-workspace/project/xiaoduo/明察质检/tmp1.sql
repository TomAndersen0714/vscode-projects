SELECT
    plat_goods_id AS `商品ID`,
    plat_goods_name AS `商品名称`,
    category_name AS `商品分类`,
    recv_cnt_sum AS `咨询人数`,
    recv_pv_sum AS `咨询量`,
    no_repl_cnt_sum AS `未回复数`,
    auto_repl_cnt_sum AS `自动回复数`,
    reply_pct AS `应答率`,
    pemt_cnt_sum AS `成交人数`,
    order_convert_pct AS `订单转化率`
FROM (
    SELECT
        plat_goods_id,
        sum(recv_cnt) AS recv_cnt_sum,
        sum(recv_pv) AS recv_pv_sum,
        sum(no_repl_cnt) AS no_repl_cnt_sum,
        (recv_pv_sum - no_repl_cnt_sum) AS auto_repl_cnt_sum,
        if(
            length(
                splitByChar('.', toString(round(auto_repl_cnt_sum / recv_pv_sum * 100, 2)))
            ) != 2,
            concat(toString(round(auto_repl_cnt_sum / recv_pv_sum * 100, 2)), '.00%'),
            if(
                length(
                    splitByChar('.', toString(round(auto_repl_cnt_sum / recv_pv_sum * 100, 2))) [2]
                ) = 1,
                concat(toString(round(auto_repl_cnt_sum / recv_pv_sum * 100, 2)), '0%'),
                concat(toString(round(auto_repl_cnt_sum / recv_pv_sum * 100, 2)), '%')
            )
        ) AS reply_pct,
        sum(pemt_cnt) AS pemt_cnt_sum,
        if(
            length(
                splitByChar('.', toString(round(pemt_cnt_sum / recv_cnt_sum * 100, 2)))
            ) != 2,
            concat(toString(round(pemt_cnt_sum / recv_cnt_sum * 100, 2)), '.00%'),
            if(
                length(
                    splitByChar('.', toString(round(pemt_cnt_sum / recv_cnt_sum * 100, 2))) [2]
                ) = 1,
                concat(toString(round(pemt_cnt_sum / recv_cnt_sum * 100, 2)), '0%'),
                concat(toString(round(pemt_cnt_sum / recv_cnt_sum * 100, 2)), '%')
            )
        ) AS order_convert_pct
    FROM dws.shop_goods_stat_all
    WHERE shop_id = '{{ shop_id=571a007989bc463220beb677 }}'
        AND day BETWEEN toYYYYMMDD(
            toDate('{{ day.start=week_ago }}')
        ) AND toYYYYMMDD(
            toDate('{{ day.end=yesterday }}')
        )
        AND plat_goods_name != ''
    GROUP BY plat_goods_id
) AS shop_goods_stat
GLOBAL LEFT JOIN (
    SELECT plat_goods_id,
        plat_goods_name,
        category_name
    FROM (
        SELECT shop_id,
            plat_goods_id,
            plat_goods_name,
            plat_cid
        FROM dim.goods_all
        WHERE shop_id = '{{ shop_id=571a007989bc463220beb677 }}'
    )
    GLOBAL INNER JOIN (
        SELECT shop_id,
            cid AS plat_cid,
            name AS category_name
        FROM dim.shop_cid_sync_all
        WHERE shop_id = '{{ shop_id=571a007989bc463220beb677 }}'
    ) USING (shop_id, plat_cid)
) AS plat_goods_info
USING (plat_goods_id)