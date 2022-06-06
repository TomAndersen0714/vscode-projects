SELECT
    plat_goods_id AS `商品ID`,
    plat_goods_name AS `商品名称`,
    category_name AS `商品分类`,
    sum(recv_cnt) AS `咨询人数`,
    sum(recv_pv) AS `咨询量`,
    (sum(recv_pv) - sum(no_repl_cnt)) AS `自动回复数`,
    if(
        length(
            splitByChar('.', toString(round(`自动回复数` / `咨询量` * 100, 2)))
        ) != 2,
        concat(toString(round(`自动回复数` / `咨询量` * 100, 2)), '.00%'),
        if(
            length(
                splitByChar('.', toString(round(`自动回复数` / `咨询量` * 100, 2))) [2]
            ) = 1,
            concat(toString(round(`自动回复数` / `咨询量` * 100, 2)), '0%'),
            concat(toString(round(`自动回复数` / `咨询量` * 100, 2)), '%')
        )
    ) AS `应答率`,
    sum(pemt_cnt) AS `成交人数`,
    if(
        length(
            splitByChar('.', toString(round(`成交人数` / `咨询人数` * 100, 2)))
        ) != 2,
        concat(toString(round(`成交人数` / `咨询人数` * 100, 2)), '.00%'),
        if(
            length(
                splitByChar('.', toString(round(`成交人数` / `咨询人数` * 100, 2))) [2]
            ) = 1,
            concat(toString(round(`成交人数` / `咨询人数` * 100, 2)), '0%'),
            concat(toString(round(`成交人数` / `咨询人数` * 100, 2)), '%')
        )
    ) AS `订单转化率`
FROM (
    SELECT
        day,
        shop_id,
        plat_goods_id,
        recv_cnt,
        recv_pv,
        pemt_cnt,
        no_repl_cnt
    FROM dws.shop_goods_stat_all
    WHERE shop_id = '{{ shop_id=571a007989bc463220beb677 }}'
        AND day BETWEEN toYYYYMMDD(
            toDate('{{ day.start=week_ago }}')
        ) AND toYYYYMMDD(
            toDate('{{ day.end=yesterday }}')
        )
        AND plat_goods_name != ''
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
    INNER JOIN (
        SELECT shop_id,
            cid AS plat_cid,
            name AS category_name
        FROM dim.shop_cid_sync_all
        WHERE shop_id = '{{ shop_id=571a007989bc463220beb677 }}'
    ) USING (shop_id, plat_cid)
) AS plat_goods_info
USING (plat_goods_id)
GROUP BY plat_goods_id, plat_goods_name, category_name