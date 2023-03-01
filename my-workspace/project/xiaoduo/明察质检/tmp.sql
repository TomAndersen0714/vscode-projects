select `day`,
    shop_id,
    buyer_nick,
    multiIf(
        2cycle = 2 and 7cycle = 7,
        '27都有',
        2cycle = 2 and 7cycle != 7,
        '有2无7',
        2cycle != 2 and 7cycle = 7,
        '有7无2',
        '其他'
    ) as tag
from (
    select distinct `day`,
        shop_id,
        buyer_nick,
        `cycle` as 2cycle
    FROM ft_dwd.ask_order_cov_detail_all
    WHERE shop_id = '{{shop_id}}'
        and day = 20230213
        and `cycle` = 2
)
full outer join (
    select distinct `day`,
        shop_id,
        buyer_nick,
        `cycle` as 7cycle
    FROM ft_dwd.ask_order_cov_detail_all
    WHERE shop_id = '{{shop_id}}'
        and day = 20230213
        and `cycle` = 7
)
using `day`, shop_id, buyer_nick