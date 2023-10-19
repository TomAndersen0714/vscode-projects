SELECT task_id,
    shop_id,
(paid_payment + (payment / 100)) as paid_payment
FROM (
        SELECT *
        FROM app_fishpond.fishpond_task_stat
    ) AS aa
    JOIN (
        SELECT task_id,
            shop_id,
            sum(payment) as payment
        FROM (
                SELECT t.task_id AS task_id,
                    o.shop_id,
                    o.order_id,
                    o.payment AS payment
                FROM (
                        SELECT shop_id,
                            order_id,
                            max(payment) AS payment
                        FROM ods.order_event_all
                        WHERE `day` BETWEEN { day_15 } AND { day }
                            AND order_id IN (
                                SELECT DISTINCT order_id
                                FROM ods.fishpond_conversion_all
                                WHERE `day` = { day_15 }
                                    AND status = 'paid'
                                    AND payment = 0
                            )
                        GROUP BY shop_id,
                            order_id
                    ) AS o
                    JOIN (
                        SELECT DISTINCT shop_id,
                            order_id,
                            task_id
                        FROM ods.fishpond_conversion_all
                        WHERE `day` = { day_15 }
                            AND status = 'paid'
                            AND payment = 0
                    ) AS t USING(
                        shop_id,
                        order_id
                    )
            )
        GROUP BY shop_id,
            task_id
    ) AS a USING(
        shop_id,
        task_id
    )