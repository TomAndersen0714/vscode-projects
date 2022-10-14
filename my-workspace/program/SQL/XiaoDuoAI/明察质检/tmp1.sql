DROP TABLE ods.order_event_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ods.order_event_all ON CLUSTER cluster_3s_2r(
    `order_id` String,
    `shop_id` String,
    `buyer_nick` String,
    `payment` Int64,
    `status` String,
    `original_status` String,
    `time` DateTime64(6),
    `plat_goods_ids` Array(String),
    `day` Int32,
    `plat_goods_names` Array(String),
    `plat_goods_price` Array(String),
    `plat_goods_count` Array(String),
    `balance_used` String,
    `seller_remark` String,
    `buyer_remark` String,
    `seller_id` String,
    `seller_discount` String,
    `order_seller_price` String,
    `pay_type` String,
    `freight_price` String
) ENGINE = Distributed(
    'cluster_3s_2r',
    'ods',
    'jd_order_event_local',
    rand()
)