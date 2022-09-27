CREATE TABLE ft_dwd.order_detail_all (
    `day` Int32,
    `platform` String,
    `shop_id` String,
    `order_id` String,
    `post_fee` Float64,
    `buyer_nick` String,
    `real_buyer_nick` String,
    `status` String,
    `original_sratus` String,
    `order_payment` Float64,
    `order_seller_price` Float64,
    `goods_id` String,
    `goods_title` String,
    `goods_price` Float64,
    `goods_num` Int32,
    `goods_payment` Float64,
    `goods_seller_price` Float64,
    `step_trade_status` String,
    `step_paid_fee` String,
    `order_type` String,
    `modified` String
) ENGINE = Distributed(
    'cluster_3s_2r',
    'ft_dwd',
    'order_detail_local',
    rand()
)