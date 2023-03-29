INSERT INTO dwd.voc_cnick_list_all
SELECT
    day,
    cnick,
    real_buyer_nick,
    rowNumberInAllBlocks() AS cnick_id
FROM (
    SELECT DISTINCT
        {ds_nodash} AS day,
        replaceOne(cnick,'cnjd','') AS cnick,
        '' AS real_buyer_nick
    FROM ods.xdrs_logs_all
    WHERE day = {ds_nodash}
    AND shop_id IN {VOC_SHOP_IDS}
) AS today_cnick_list
WHERE cnick NOT IN (
    SELECT cnick
    FROM dwd.voc_cnick_list_all
    WHERE day = {yesterday_ds_nodash}
)
