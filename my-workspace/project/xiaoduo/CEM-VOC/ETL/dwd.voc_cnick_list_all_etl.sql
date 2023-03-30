INSERT INTO dwd.voc_cnick_list_all
WITH (
    SELECT max(cnick_id) + 1
    FROM {dwd_voc_cnick_etl_sink_table}
    WHERE day = {yesterday_ds_nodash}
    AND platform = '{platform}'
) AS max_cnick_id
SELECT
    day,
    '{platform}' AS platform,
    cnick,
    real_buyer_nick,
    max_cnick_id + rowNumberInAllBlocks() AS cnick_id
FROM (
    SELECT DISTINCT
        {ds_nodash} AS day,
        replaceOne(cnick,'cnjd','') AS cnick,
        '' AS real_buyer_nick
    FROM ods.xdrs_logs_all
    WHERE day = {ds_nodash}
    AND shop_id IN {shop_ids}
) AS today_cnick_list
WHERE cnick NOT IN (
    -- 剔除已有子账号记录
    SELECT cnick
    FROM dwd.voc_cnick_list_all
    WHERE day = {yesterday_ds_nodash}
    AND platform = '{platform}'
) AS yesterday_cnick_list