    SELECT
        *
    FROM ods.order_event_all
    WHERE day BETWEEN 20230301 AND 20230331
    AND shop_id = '5a48c5c489bc46387361988d'