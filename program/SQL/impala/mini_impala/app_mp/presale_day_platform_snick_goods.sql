WITH x1 AS (
        SELECT seller_nick,
                concat('cntaobao', buyer_nick) AS buyer_nick,
                tid AS order_id,
                iid,
                pay_ment,
                modified,
                row_number() OVER (
                        PARTITION BY tid
                        ORDER BY modified DESC
                ) AS seq_num
        FROM ods.tb_order
        WHERE DAY IN (
                        cast(replace('{{ macros.ds_add(ds,params.interval) }}', '-', '') as int),
                        cast(
                                replace(
                                        to_date(days_add('{{ macros.ds_add(ds,params.interval) }}', 1)),
                                        '-',
                                        ''
                                ) as int
                        )
                )
                AND status = "WAIT_SELLER_SEND_GOODS"
),
x2 AS (
        SELECT seller_nick,
                buyer_nick,
                order_id,
                iid,
                cast(pay_ment AS float) AS payment
        FROM x1
        WHERE seq_num = 1
),
x3 AS (
        SELECT x2.*,
                if(plat_goods_id IS NOT NULL, plat_goods_id, iid) AS plat_goods_id,
                shop_id
        FROM x2
                LEFT JOIN ods.order_goods_ck ON x2.order_id = ods.order_goods_ck.order_id
        WHERE ods.order_goods_ck.`day` IN (
                        cast(replace('{{ macros.ds_add(ds,params.interval) }}', '-', '') as int),
                        cast(
                                replace(
                                        to_date(days_add('{{ macros.ds_add(ds,params.interval) }}', 1)),
                                        '-',
                                        ''
                                ) as int
                        )
                )
),
x4 AS (
        SELECT shop_id,
                seller_nick,
                buyer_nick,
                plat_goods_id,
                count(order_id) as order_count,
                sum(payment) as payment
        FROM x3
        group by 1,
                2,
                3,
                4
),
y1 AS (
        SELECT shop_id,
                split_part(snick, ':', 1) AS seller_nick,
                cnick,
                plat_goods_id
        FROM dwd.mini_xdrs_log
        WHERE `day` IN (
                        cast(replace('{{ macros.ds_add(ds,params.interval) }}', '-', '') as int)
                )
                AND act = "recv_msg"
                AND platform = 'tb'
                AND plat_goods_id != ""
),
y2 AS (
        SELECT shop_id,
                seller_nick,
                cnick,
                plat_goods_id,
                count(1) AS pv
        FROM y1
        GROUP BY 1,
                2,
                3,
                4
),
z1 AS (
        SELECT y2.*,
                1 AS reception_uv,
                IF(x4.seller_nick IS NULL, 0, 1) AS paid_uv,
                IF(x4.payment IS NULL, 0, payment) AS payment
        FROM y2
                LEFT JOIN x4 ON y2.shop_id = x4.shop_id
                AND y2.cnick = x4.buyer_nick
                AND y2.plat_goods_id = x4.plat_goods_id
),
ask_count_info AS (
        SELECT snick_oid AS shop_id,
                snick,
                plat_goods_id,
                sum(ask_count) AS ask_count
        FROM app_mp.presale_day_platform_snick_goods_question
        WHERE `day` IN (
                        cast(replace('{{ macros.ds_add(ds,params.interval) }}', '-', '') as int)
                )
        group by 1,
                2,
                3
),
z2 AS (
        SELECT seller_nick,
                shop_id,
                plat_goods_id,
                sum(pv) AS pv,
                sum(reception_uv) AS reception_uv,
                sum(paid_uv) AS paid_uv,
                sum(payment) AS payment
        FROM z1
        GROUP BY 1,
                2,
                3
),
z3 AS (
        SELECT z2.seller_nick,
                z2.shop_id,
                z2.plat_goods_id,
                ask_count,
                reception_uv,
                paid_uv,
                payment
        FROM z2
                left join ask_count_info using(shop_id, plat_goods_id)
)
INSERT overwrite app_mp.presale_day_platform_snick_goods partition(DAY)
SELECT "{{ macros.ds_add(ds,params.interval) }}",
        "tb",
        seller_nick AS snick,
        shop_id,
        plat_goods_id,
        ask_count,
        reception_uv,
        paid_uv,
        payment,
        cast(replace('{{ macros.ds_add(ds,params.interval) }}', '-', '') as int)
FROM z3
where ask_count != 0;