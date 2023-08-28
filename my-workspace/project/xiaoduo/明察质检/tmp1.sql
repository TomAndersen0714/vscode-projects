SELECT
    day,
    company_id,
    shop_id,
    platform,
    tag,
    order_status,
    goods_id,
    groupBitmapState(cnick_id) AS cnick_id_bitmap
FROM (
    SELECT
        day,
        company_id,
        shop_id,
        platform,
        arrayJoin(if(empty(tags), [-1], tags)) AS tag,
        order_status,
        arrayJoin(if(empty(dialog_info_goods_ids), [''], dialog_info_goods_ids)) AS goods_id,
        cnick
    FROM ods.voc_customer_all
    WHERE day = 20230826
) AS ods_voc_customer
-- 获取cnick的one_id
LEFT JOIN (
    SELECT
        platform,
        cnick,
        cnick_id
    FROM dwd.voc_cnick_list_latest_all
    -- 筛选当日咨询客户
    WHERE (platform, cnick) IN (
        SELECT
            platform, 
            cnick
        FROM ods.voc_customer_all
        WHERE day = 20230826
    )
) AS cnick_one_id
USING(platform, cnick)
GROUP BY
    day, company_id, shop_id, platform, tag, order_status, goods_id