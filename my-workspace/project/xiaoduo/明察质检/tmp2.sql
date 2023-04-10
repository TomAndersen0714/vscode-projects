select t1.buyer_nick,
    created_day
from (
        select buyer_nick,
            created_day
        from ft_dwd.new_ask_order_cov_buyer_detail_all
        where cycle = 2
            and shop_id = '5cac112e98ef4100118a9c9f' -- and act='paid'
            -- and created_day<=20230401
            and update_time <= '2023-03-21' -- and buyer_nick like '%one%'
            and paid_time <> ''
    ) as t1
    left join (
        select distinct buyer_nick
        from ft_dwd.new_ask_order_cov_buyer_snick_detail_all
        where cycle = 2
            and shop_id = '5cac112e98ef4100118a9c9f'
            and act = 'paid' -- and snick='方太官方旗舰店:七七'
            and buyer_nick like '%one%'
            and created_day <= 20230319
    ) as t2 on t1.buyer_nick = t2.buyer_nick
where t2.buyer_nick = ''
order by created_day desc
limit 1000