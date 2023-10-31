SELECT
    task_id, shop_id, platform, paid_pv, paid_uv, paid_payment
FROM (
    SELECT task_id,
        shop_id,
        platform,
        count(1) AS paid_pv,
        uniqExact(cnick) AS paid_uv,
        sum(payment) AS paid_payment
    FROM (
        SELECT DISTINCT cnick,
            task_id,
            shop_id,
            platform,
            payment,
            order_id
        FROM ods.fishpond_conversion_all
        WHERE `day` >= toYYYYMMDD(addDays(today(), -20))
            AND `day` <= toYYYYMMDD(today())
            AND platform = 'dy'
            AND status = 'paid'
    ) AS etl_order
    GROUP BY task_id, shop_id, platform
)
WHERE task_id GLOBAL IN (
    SELECT _id
    FROM dim.fishpond_task_all
    WHERE toYYYYMMDD(today()) BETWEEN `day`
        AND toYYYYMMDD(addDays(parseDateTimeBestEffort(left(send_time_end, 10)),5))
        AND platform = 'dy'
)