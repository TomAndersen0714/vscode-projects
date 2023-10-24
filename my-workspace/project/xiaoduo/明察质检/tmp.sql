CREATE TABLE IF NOT EXISTS buffer.dy_order_event_buffer ON CLUSTER cluster_3s_2r (
    `order_id` String,
    `shop_id` String,
    `buyer_nick` String,
    `payment` Int64,
    `status` String,
    `original_status` String,
    `time` DateTime64(6),
    `plat_goods_ids` Array(String),
    `day` Int32
) ENGINE = Buffer(
    'ods',
    'dy_order_event_all',
    16,
    10,
    30,
    81920,
    409600,
    16777216,
    67108864
) 


CREATE TABLE IF NOT EXISTS ods.dy_order_event_all ON CLUSTER cluster_3s_2r (
    `order_id` String,
    `shop_id` String,
    `buyer_nick` String,
    `payment` Int64,
    `status` String,
    `original_status` String,
    `time` DateTime64(6),
    `plat_goods_ids` Array(String),
    `day` Int32
) ENGINE = Distributed(
    'cluster_3s_2r',
    'ods',
    'dy_order_event_local',
    xxHash64(shop_id, order_id, status, original_status)
)




