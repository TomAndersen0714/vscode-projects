SELECT DISTINCT
    cnick
FROM (
    SELECT
        replaceOne(cnick,'cntaobao','') AS cnick
    FROM ods.xdrs_logs_all
    WHERE day = 20230826
    AND shop_id = '60b72d421edc070017428380'
    AND act IN ['send_msg', 'recv_msg']
    AND plat_goods_id = '718154483681'
)