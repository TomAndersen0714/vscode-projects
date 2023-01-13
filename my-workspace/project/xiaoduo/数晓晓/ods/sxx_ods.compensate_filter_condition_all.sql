CREATE DATABASE sxx_ods ON CLUSTER cluster_3s_2r

-- DROP TABLE sxx_ods.compensate_filter_condition_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_ods.compensate_filter_condition_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `raw_info` String,
    `sequence_num` Int64,
    `field_name` String,
    `filter_value` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY (day, platform)
ORDER BY (field_name, filter_value)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE sxx_ods.compensate_filter_condition_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_ods.compensate_filter_condition_all ON CLUSTER cluster_3s_2r
AS sxx_ods.compensate_filter_condition_local
ENGINE = Distributed('cluster_3s_2r', 'sxx_ods', 'compensate_filter_condition_local', rand())

-- DROP TABLE buffer.sxx_ods_compensate_filter_condition_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.sxx_ods_compensate_filter_condition_buffer ON CLUSTER cluster_3s_2r
AS sxx_ods.compensate_filter_condition_all
ENGINE = Buffer('sxx_ods', 'compensate_filter_condition_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)


INSERT INTO TABLE sxx_ods.compensate_filter_condition_all(day, platform, shop_id, shop_name, sequence_num, field_name, filter_value)
VALUES
(20220705, 'tb', '6041c9e855c9f9012a1054f6', '红小厨旗舰店', 1, 'reason_level_3', '退差价')
(20220705, 'tb', '6041c9e855c9f9012a1054f6', '红小厨旗舰店', 2, 'reason_level_3', '修改差评')
(20220705, 'tb', '6041c9e855c9f9012a1054f6', '红小厨旗舰店', 3, 'reason_level_3', '好评返现')
(20220705, 'tb', '6041c9e855c9f9012a1054f6', '红小厨旗舰店', 4, 'reason_level_3', '直播中奖')
(20220705, 'tb', '6041c9e855c9f9012a1054f6', '红小厨旗舰店', 5, 'custom_goods', '外婆菜')
(20220705, 'tb', '6041c9e855c9f9012a1054f6', '红小厨旗舰店', 6, 'custom_goods', '火锅底料')
(20220705, 'tb', '6041c9e855c9f9012a1054f6', '红小厨旗舰店', 7, 'custom_goods', '意大利面')
(20220705, 'tb', '6041c9e855c9f9012a1054f6', '红小厨旗舰店', 8, 'custom_goods', '鸭脖')
(20220705, 'tb', '6041c9e855c9f9012a1054f6', '红小厨旗舰店', 9, 'reason_level_3', '仓库丢单')
(20220705, 'tb', '6041c9e855c9f9012a1054f6', '红小厨旗舰店', 10, 'reason_level_3', '超时发货')
(20220705, 'tb', '6041c9e855c9f9012a1054f6', '红小厨旗舰店', 11, 'warehouse', '虚拟仓')
(20220705, 'tb', '6041c9e855c9f9012a1054f6', '红小厨旗舰店', 12, 'warehouse', '萧山协作仓')
(20220705, 'tb', '6041c9e855c9f9012a1054f6', '红小厨旗舰店', 13, 'warehouse', '代发仓')
(20220705, 'tb', '6041c9e855c9f9012a1054f6', '红小厨旗舰店', 14, 'custom_goods', '大闸蟹')
(20220705, 'tb', '6041c9e855c9f9012a1054f6', '红小厨旗舰店', 15, 'custom_goods', '蟹卡')
(20220705, 'tb', '6041c9e855c9f9012a1054f6', '红小厨旗舰店', 16, 'custom_goods', '海鲜卡')
(20220705, 'tb', '6041c9e855c9f9012a1054f6', '红小厨旗舰店', 17, 'custom_goods', '谢云提货')