-- 统计XQC各客户店铺数, 店铺ID, 店铺名
SELECT
    company_id,
    groupArray(platform) AS platforms
    groupArray(shop_cnt) AS shop_cnts,
    groupArray(seller_nicks) AS seller_nicks_arr,
    groupArray(shop_ids) AS shop_ids_arr
FROM (
    SELECT
        company_id,
        platform,
        count(1) AS shop_cnt,
        groupArray(seller_nick) AS seller_nicks,
        groupArray(shop_id) AS shop_ids
    FROM xqc_dim.xqc_shop_all
    WHERE `day` = toYYYYMMDD(yesterday())
    GROUP BY company_id, platform
) AS company_platform_info
GROUP BY company_id