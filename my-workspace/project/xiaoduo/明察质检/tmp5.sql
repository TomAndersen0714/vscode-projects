CREATE TABLE xqc_ods.dialog_all (
    `id` String,
    `platform` String,
    `shop_id` String,
    `seller_nick` String,
    `snick` String,
    `cnick` String,
    `employee_name` String,
    `superior_name` String,
    `time` String,
    `hour` Int64,
    `day` Int64
) ENGINE = Distributed(
    'cluster_3s_2r',
    'xqc_ods',
    'dialog_local',
    rand()
)