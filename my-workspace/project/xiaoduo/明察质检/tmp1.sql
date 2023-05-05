CREATE TABLE buffer.chat_event_v1_buffer (
    `shop_id` String,
    `snick` String,
    `sub_nick` String,
    `buyer_nick` String,
    `plat_goods_id` String,
    `act` String,
    `intent` String,
    `time` DateTime('Asia/Shanghai'),
    `day` Int32,
    `one_id` String,
    `real_buyer_nick` String
) ENGINE = Buffer(
    'ods',
    'chat_event_dis',
    16,
    15,
    35,
    81920,
    409600,
    16777216,
    67108864
)