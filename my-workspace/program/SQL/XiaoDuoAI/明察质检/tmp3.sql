INSERT INTO buffer.sxx_ods_outbound_workorder_buffer
SELECT
    toYYYYMMDD(toDateTime64(outbound_info.delivery_time, 3)) AS day,
    plat_map.platform AS platform,
    plat_map.platform_cn AS platform_cn,
    '' AS shop_id,
    plat_map.shop_name AS shop_name,
    '' AS raw_info,
    outbound_info.*
FROM (
    SELECT
        *
    FROM sxx_tmp.outbound_workorder_all
) AS outbound_info
GLOBAL LEFT JOIN (
    SELECT 
        platform,
        platform_cn,
        custom_shop_name,
        shop_name
    FROM sxx_tmp.plat_shop_map_all
) AS plat_map
USING(custom_shop_name)