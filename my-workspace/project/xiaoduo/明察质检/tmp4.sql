SELECT *
FROM (
    SELECT
        day,
        platform,
        cnick,
        real_buyer_nick
    FROM (
        SELECT DISTINCT
            day,
            platform,
            replaceOne(cnick,'cntaobao','') AS cnick,
            length(cnick) AS cnick_len,
            real_buyer_nick
        FROM ods.xdrs_logs_all
        PREWHERE day = 20230826
        AND shop_id GLOBAL IN (
            SELECT shop_id
            FROM xqc_dim.shop_latest_all
            WHERE company_id GLOBAL IN (
                SELECT _id
                FROM xqc_dim.company_latest_all
                WHERE has(white_list, 'VOC')
            )
            AND platform IN ['tb']
        )
        AND shop_id = '60b72d421edc070017428380'
        AND cnick = 'one_id_397347653'
    ) AS today_cnick_list
)