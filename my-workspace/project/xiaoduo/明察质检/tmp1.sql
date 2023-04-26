CREATE TABLE xqc_dim.xqc_shop_all
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `shop_id` String,
    `platform` String,
    `seller_nick` String,
    `plat_shop_name` String,
    `plat_shop_id` String,
    `day` Int32
)
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'xqc_shop_local', rand())