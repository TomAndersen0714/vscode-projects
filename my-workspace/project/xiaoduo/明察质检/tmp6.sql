-- stage_1
INSERT INTO dwd.voc_buyer_latest_order_all
SELECT
    day,
    {platform} AS platform,
    shop_id,
    buyer_nick,
    '' AS real_buyer_nick,
    order_id,
    
    status
FROM ods.order_event_all
WHERE day = {ds_nodash}
AND shop_id IN {shop_id}
AND status = 'created'


-- stage_2
INSERT INTO dwd.voc_buyer_latest_order_all
