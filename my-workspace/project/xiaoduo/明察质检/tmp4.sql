SELECT *
FROM (
        SELECT task_id,
            shop_id,
            count(1) AS paid_pv,
            uniqExact(cnick) AS paid_uv,
            sum(payment) AS paid_payment
        from (
                select distinct cnick,
                    task_id,
                    shop_id,
                    payment,
                    order_id
                FROM ods.fishpond_conversion_all
                WHERE `day` >= toYYYYMMDD(addDays(today(), -20))
                    AND `day` <= toYYYYMMDD(today())
                    AND platform = 'dy'
                    AND status = 'paid'
            ) as etl_order
        GROUP BY shop_id,
            task_id
    )
WHERE task_id GLOBAL IN (
        select _id
        from dim.fishpond_task_all
        where toYYYYMMDD(today()) between `day` and toYYYYMMDD(
                addDays(
                    parseDateTimeBestEffort(left(send_time_end, 10)),
                    5
                )
            )
    )