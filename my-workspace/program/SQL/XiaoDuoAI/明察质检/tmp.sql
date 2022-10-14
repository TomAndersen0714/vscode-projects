CREATE DATABASE IF NOT EXISTS ods ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE ods.trade_order_event_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ods.trade_order_event_local ON CLUSTER cluster_3s_2r
(
    `platform` String,
    `order_id` String,
    `shop_id` String,
    `buyer_nick` String,
    `real_buyer_nick` String,
    `payment` String,
    `buyer_remark` String,
    `status` String,
    `created_at` Int64,
    `updated_at` Int64,
    `item_ids` Array(String),
    `origin_status` String,
    `step_trade_status` String,
    `seller_memo` String,
    `step_paid_fee` String,
    `status_history` Array(String),
    `original_order_id` String,
    `tbext_info` String,
    `original_order_info` String,
    `original_order_orders_info` String,
    `original_order_orders_order_arr` Array(String),
    `original_order_adjust_fee_arr` Array(String),
    `original_order_cid_id_arr` Array(String),
    `original_order_discount_fee_arr` Array(String),
    `original_order_nr_outer_iid_arr` Array(String),
    `original_order_logistics_company_arr` Array(String),
    `original_order_num_arr` Array(String),
    `original_order_num_iid_arr` Array(String),
    `original_order_oid_arr` Array(String),
    `original_order_outer_sku_id_arr` Array(String),
    `original_order_part_mjz_discount_arr` Array(String),
    `original_order_divide_order_fee_arr` Array(String),
    `original_order_payment_arr` Array(String),
    `original_order_price_arr` Array(String),
    `original_order_refund_status_arr` Array(String),
    `original_order_sku_id_arr` Array(String),
    `original_order_status_arr` Array(String),
    `original_order_store_code_arr` Array(String),
    `original_order_title_arr` Array(String),
    `original_order_total_fee_arr` Array(String),
    `original_order_adjust_fee` String,
    `original_order_buyer_cod_fee` String,
    `original_order_coupon_fee` String,
    `original_order_discount_fee` String,
    `original_order_post_fee` String,
    `original_order_received_payment` String,
    `original_order_total_fee` String,
    `original_order_type` String,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (vender_id, order_id, order_update_time, order_status )
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE ods.trade_order_event_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ods.trade_order_event_all ON CLUSTER cluster_3s_2r
AS ods.trade_order_event_local
ENGINE = Distributed('cluster_3s_2r', 'ods', 'trade_order_event_local', rand())

-- DROP TABLE buffer.ods_trade_order_event_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.ods_trade_order_event_buffer ON CLUSTER cluster_3s_2r
AS ods.trade_order_event_all
ENGINE = Buffer('ods', 'trade_order_event_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)