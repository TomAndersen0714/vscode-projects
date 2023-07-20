SELECT
    company_id, company_name, company_short_name,
    shop_id, shop_name, seller_nick
FROM (
    SELECT
        _id AS company_id,
        name AS company_name,
        shot_name AS AS company_short_name
    FROM xqc_dim.company_latest_all
) AS company_info
GLOBAL LEFT JOIN (
    SELECT
        company_id, shop_id, seller_nick
    FROM xqc_dim.shop_latest_all
) AS shop_info
USING(company_id)