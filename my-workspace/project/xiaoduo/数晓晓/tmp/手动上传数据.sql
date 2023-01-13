-- DELETE
TRUNCATE TABLE sxx_tmp.outbound_workorder_local ON CLUSTER cluster_3s_2r
ALTER TABLE sxx_tmp.outbound_workorder_local ON CLUSTER cluster_3s_2r DELETE WHERE toYYYYMM(toDateTime64(delivery_time, 3)) = 202207

-- INSERT INTO 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query=\
"INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 1.snappy.parquet

-- check
SELECT * FROM sxx_tmp.outbound_workorder_all LIMIT 100

-- DELETE 
-- TRUNCATE TABLE sxx_ods.outbound_workorder_local ON CLUSTER cluster_3s_2r
ALTER TABLE sxx_ods.outbound_workorder_local ON CLUSTER cluster_3s_2r DELETE WHERE toUInt32(day/100) = 202207

-- INSERT INTO
INSERT INTO buffer.sxx_ods_outbound_workorder_buffer
SELECT
    toYYYYMMDD(toDateTime64(outbound_info.delivery_time, 3)) AS day,
    plat_map.platform AS platform,
    plat_map.platform_cn AS platform_cn,
    '' AS shop_id,
    plat_map.shop_name AS shop_name,
    '' AS raw_info,
    order_id,
    origin_id,
    origin_sub_id,
    sub_origin_id,
    order_type,
    paid_account,
    workorder_id,
    warehouse,
    custom_shop_name,
    workorder_status,
    outbound_status,
    sorting_id,
    business_id,
    product_id,
    product_name,
    product_short_name,
    brand,
    classification,
    specification_code,
    specification_name,
    barcode,
    product_cnt,
    product_unit_price*100,
    product_price*100,
    order_total_discounts*100,
    order_postage*100,
    shared_postage*100,
    product_final_price*100,
    product_total_final_price*100,
    product_final_discounts*100,
    pay_on_delivery*100,
    to_receive_money*100,
    estimate_postage*100,
    estimate_weigh_postage*100,
    estimate_product_cost*100,
    product_cost*100,
    real_product_total_cost*100,
    custom_account,
    receiver,
    receiving_area,
    receiving_address,
    receiving_cellphone,
    receiving_telephone,
    logistics_company,
    logistics_company_abbr,
    weigh_result,
    estimate_weigh,
    invoice,
    mark,
    create_person,
    print_person,
    sorting_person,
    packing_person,
    goods_checking_person,
    goods_delivery_person,
    salesman_person,
    print_batch_id,
    logistics_order_print_status,
    delivery_order_print_status,
    sorting_batch_print_status,
    logistics_order_id,
    sorting_batch_id,
    paid_time,
    order_time,
    delivery_time,
    gift_way,
    custom_message,
    service_comment,
    print_comment,
    comment,
    packing,
    combination_id,
    combination_name,
    combination_cnt,
    outbound_label,
    order_label,
    unit_price,
    distributor_name,
    unit_weigh,
    unit_class,
    unit_attribute_3,
    unit_attribute_4,
    unit_attribute_5,
    unit_attribute_6,
    product_attribute_1,
    product_attribute_2,
    product_attribute_3,
    product_attribute_4,
    product_attribute_5,
    product_attribute_6,
    actual_outbound_weigh,
    combination_class,
    combination_attribute_3,
    combination_attribute_4
FROM (
    SELECT
        *
    FROM sxx_tmp.outbound_workorder_all
    -- WHERE toYYYYMM(toDateTime64(delivery_time, 3)) = 202207
) AS outbound_info
GLOBAL LEFT JOIN (
    SELECT DISTINCT
        platform,
        platform_cn,
        custom_shop_name,
        shop_name
    FROM sxx_tmp.plat_shop_map_all
) AS plat_map
USING(custom_shop_name)

-- check
SELECT * FROM sxx_ods.outbound_workorder_all LIMIT 100