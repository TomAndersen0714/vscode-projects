CREATE DATABASE sxx_ods ON CLUSTER cluster_3s_2r

-- DROP TABLE sxx_ods.outbound_workorder_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_ods.outbound_workorder_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `raw_info` String,
    `order_id` String,
    `origin_id` String,
    `origin_sub_id` String,
    `sub_origin_id` String,
    `order_type` String,
    `paid_account` String,
    `workorder_id` String,
    `warehouse` String,
    `custom_shop_name` String,
    `workorder_status` String,
    `outbound_status` String,
    `sorting_id` String,
    `business_id` String,
    `product_id` String,
    `product_name` String,
    `product_short_name` String,
    `brand` String,
    `classification` String,
    `specification_code` String,
    `specification_name` String,
    `barcode` String,
    `product_cnt` Int64,
    `product_unit_price` Int64,
    `product_price` Int64,
    `order_total_discounts` Int64,
    `order_postage` Int64,
    `shared_postage` Int64,
    `product_final_price` Int64,
    `product_total_final_price` Int64,
    `product_final_discounts` Int64,
    `pay_on_delivery` Int64,
    `to_receive_money` Int64,
    `estimate_postage` Int64,
    `estimate_weigh_postage` Int64,
    `estimate_product_cost` Int64,
    `product_cost` Int64,
    `real_product_total_cost` Int64,
    `custom_account` String,
    `receiver` String,
    `receiving_area` String,
    `receiving_address` String,
    `receiving_cellphone` String,
    `receiving_telephone` String,
    `logistics_company` String,
    `logistics_company_abbr` String,
    `weigh_result` String,
    `estimate_weigh` Float64,
    `invoice` String,
    `mark` String,
    `create_person` String,
    `print_person` String,
    `sorting_person` String,
    `packing_person` String,
    `goods_checking_person` String,
    `goods_delivery_person` String,
    `salesman_person` String,
    `print_batch_id` String,
    `logistics_order_print_status` String,
    `delivery_order_print_status` String,
    `sorting_batch_print_status` String,
    `logistics_order_id` String,
    `sorting_batch_id` String,
    `paid_time` String,
    `order_time` String,
    `delivery_time` String,
    `gift_way` String,
    `custom_message` String,
    `service_comment` String,
    `print_comment` String,
    `comment` String,
    `packing` String,
    `combination_id` String,
    `combination_name` String,
    `combination_cnt` String,
    `outbound_label` String,
    `order_label` String,
    `unit_price` String,
    `distributor_name` String,
    `unit_weigh` String,
    `unit_class` String,
    `unit_attribute_3` String,
    `unit_attribute_4` String,
    `unit_attribute_5` String,
    `unit_attribute_6` String,
    `product_attribute_1` String,
    `product_attribute_2` String,
    `product_attribute_3` String,
    `product_attribute_4` String,
    `product_attribute_5` String,
    `product_attribute_6` String,
    `actual_outbound_weigh` Float64,
    `combination_class` String,
    `combination_attribute_3` String,
    `combination_attribute_4` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY (day, platform)
ORDER BY (order_id, product_name)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE sxx_ods.outbound_workorder_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_ods.outbound_workorder_all ON CLUSTER cluster_3s_2r
AS sxx_ods.outbound_workorder_local
ENGINE = Distributed('cluster_3s_2r', 'sxx_ods', 'outbound_workorder_local', rand())

-- DROP TABLE buffer.sxx_ods_outbound_workorder_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.sxx_ods_outbound_workorder_buffer ON CLUSTER cluster_3s_2r
AS sxx_ods.outbound_workorder_all
ENGINE = Buffer('sxx_ods', 'outbound_workorder_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)