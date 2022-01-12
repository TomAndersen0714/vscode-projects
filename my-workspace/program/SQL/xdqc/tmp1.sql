select buyer_nick,
    sum(payment) as sum_payment_paid
from ods.order_event_all
where shop_id = '5de650c946e7c3001814990f'
    and status = 'paid'
    and `day` >= 20210106
    and `day` <= 20211231
    and `time` >= '2021-01-06 00:00:00'
    and `time` <= '2021-12-31 23:59:59'
group by buyer_nick
having sum_payment_paid > 1500