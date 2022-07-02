INSERT INTO {ch_sink_table}(day, platform, shop_id, order_id, warehouse_type)
SELECT
    day,
    'jd' AS platform,
    shop_id,
    order_id,
    '京东仓' AS warehouse_type
FROM {ch_jd_order_table}
WHERE day = {ds_nodash}
AND shop_id GLOABL IN (
    '红小厨旗舰店',
    '星农联合京东自营官方旗舰店',
    '红小厨京东自营旗舰店',
    '星农联合官方旗舰店',
    '红小厨生鲜旗舰店',
    '阳澄联合京东自营官方旗舰店'
)