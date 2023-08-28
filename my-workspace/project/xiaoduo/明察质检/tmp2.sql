WITH (
    SELECT max(cnick_id) + 1
    FROM dwd.voc_cnick_list_all
    WHERE day = 20230825
) AS max_cnick_id
SELECT
    day,
    platform,
    cnick,
    real_buyer_nick,
    max_cnick_id + rowNumberInAllBlocks() AS cnick_id
FROM (
    SELECT DISTINCT
        day,
        platform,
        replaceOne(cnick,'cntaobao','') AS cnick,
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
) AS today_cnick_list
WHERE (platform, cnick) NOT IN (
    -- 剔除历史咨询买家
    SELECT platform, cnick
    FROM dwd.voc_cnick_list_all
    WHERE day = 20230825
    AND platform IN ['tb']
) AS yesterday_cnick_list