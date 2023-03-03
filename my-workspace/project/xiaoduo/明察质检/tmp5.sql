SELECT DISTINCT buyer_nick
FROM ft_dwd.order_detail_all
WHERE `day` BETWEEN toYYYYMMDD(
        subtractDays(subtractDays(toDate('{{ds}}'), { { cycle } } - 1), 180)
    ) AND toYYYYMMDD(subtractDays(toDate('{{ds}}'), { { cycle } } - 1))
    AND shop_id = '{{shop_id}}'