SELECT tmp.*
FROM (
        SELECT *
        FROM { tmp_table }
        WHERE `day` = { ds_nodash }
    ) AS tmp
    LEFT JOIN (
        SELECT *
        FROM { sink_table }
        WHERE `day` = { ds_nodash }
    ) AS ods USING(
        `day`,
        shop_id,
        order_id,
        order_status
    )
HAVING ods.order_id = ''