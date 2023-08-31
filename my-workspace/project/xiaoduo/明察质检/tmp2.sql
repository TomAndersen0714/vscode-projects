SELECT
    groupBitmapOr(cnick_id_bitmap)
FROM (
    SELECT
        cnick_id_bitmap
    FROM dws.voc_goods_question_stat_all
    WHERE day = 20230826
    AND shop_id = '60b72d421edc070017428380'
    AND plat_goods_id = '718154483681'
)