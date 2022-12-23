upsert into ods.goods (
    platform,
    plat_goods_id,
    shop_id,
    _id,
    create_time,
    plat_goods_name,
    plat_goods_url,
    plat_goods_img
)
select 'jd' as platform,
    plat_goods_id,
    shop_id,
    goods_id,
    create_time,
    plat_goods_name,
    plat_goods_url,
    plat_goods_img
from tmp.jd_goods