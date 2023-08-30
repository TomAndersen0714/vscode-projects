select count(1) as count
from (
        select distinct buyer_nick
        from (
                select distinct final_table_0.buyer_nick
                from (
                        select distinct buyer_nick
                        from (
                                select buyer_nick,
                                    avg(payment) as avg_payment_paid
                                from ods.order_event_all
                                where shop_id = '5ec76879edbe97000f8d850c'
                                    and status = 'paid'
                                group by buyer_nick
                                having avg_payment_paid between 100 and 100
                            )
                    ) as final_table_0
            ) as table_groupresult
            join (
                SELECT DISTINCT buyer_nick
                FROM (
                        SELECT DISTINCT buyer_nick
                        FROM ods.order_event_all
                        WHERE `day` > toYYYYMMDD(subtractDays(now(), 60))
                            AND shop_id = '5ec76879edbe97000f8d850c'
                            AND status = 'created'
                        UNION ALL
                        SELECT DISTINCT buyer_nick
                        FROM ods.chat_event_all
                        WHERE `day` > toYYYYMMDD(subtractDays(now(), 60))
                            AND shop_id = '5ec76879edbe97000f8d850c'
                            AND act = 'consult'
                    )
            ) as table_reachable on table_reachable.buyer_nick = table_groupresult.buyer_nick
    ) -- trace:1028b7ca5083d232fed82ae013239fda