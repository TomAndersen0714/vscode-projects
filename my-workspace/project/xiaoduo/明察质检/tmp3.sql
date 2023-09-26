CREATE TABLE dim.shop_gray_all ON CLUSTER cluster_3s_2r(
    `_id` String,
    `platform` String,
    `shop_id` String,
    `plat_user_id` String,
    `upgrade_status` Int32,
    `upgrade_operate_time` String,
    `upgrade_operate_user` String,
    `create_time` String,
    `update_time` String,
    `upgrade_start_time` String,
    `upgrade_end_time` String
) ENGINE = Distributed(
    'cluster_3s_2r',
    'dim',
    'shop_gray_local',
    rand()
)