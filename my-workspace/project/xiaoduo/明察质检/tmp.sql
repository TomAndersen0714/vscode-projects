SELECT
    goods_id,
    groupBitmapOr(cnick_id_bitmap) AS churn_count
FROM (
    SELECT cnick_id_bitmap,
        goods_id
    FROM dws.voc_customer_stat_all
    WHERE `day` = 20230827
        AND company_id = '63fc50f0a06a5ecd9a249ac9'
        AND platform = 'tb'
        AND shop_id = '60b72d421edc070017428380'
        AND goods_id = '718154483681'
        AND order_status IN ['2', '3', '4']
)
GROUP BY goods_id


SELECT
    COUNT(DISTINCT cnick)
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
    WHERE day = 20230827
    AND company_id = '63fc50f0a06a5ecd9a249ac9'
    AND platform = 'tb'
    AND shop_id = '60b72d421edc070017428380'
    AND goods_id = '718154483681'
)