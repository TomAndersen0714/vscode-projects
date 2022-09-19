CREATE DATABASE IF NOT EXISTS ft_dwd ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE ft_dwd.transfer_msg_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_dwd.transfer_msg_local ON CLUSTER cluster_3s_2r
(
    `id` String,
    `day` Int32,
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `from_snick` String,
    `to_snick` String,
    `cnick` String,
    `real_buyer_nick` String,
    `create_time` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, shop_name)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE ft_dwd.transfer_msg_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_dwd.transfer_msg_all ON CLUSTER cluster_3s_2r
AS ft_dwd.transfer_msg_local
ENGINE = Distributed('cluster_3s_2r', 'ft_dwd', 'transfer_msg_local', rand())

-- DROP TABLE buffer.ft_dwd_transfer_msg_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.ft_dwd_transfer_msg_buffer ON CLUSTER cluster_3s_2r
AS ft_dwd.transfer_msg_all
ENGINE = Buffer('ft_dwd', 'transfer_msg_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)

-- ETL(tb)
INSERT INTO buffer.ft_dwd_transfer_msg_buffer
SELECT
    lower(hex(MD5(concat(shop_id, from_snick, to_snick, cnick, real_buyer_nick, create_time)))) AS id,
    day,
    platform,
    shop_id,
    shop_name,
    from_snick,
    to_snick,
    cnick,
    real_buyer_nick,
    create_time
FROM (
    SELECT
        day,
        platform,
        plat_user_id AS shop_name,
        from_spin AS from_snick,
        to_spin AS to_snick,
        buyer_one_id AS cnick,
        pin AS real_buyer_nick,
        create_time
    FROM ods.transfer_msg_all
    WHERE day BETWEEN 20220911 AND 20220918
    AND platform = 'tb'
    AND plat_user_id = '方太官方旗舰店'
) AS transfer_msg_info
GLOBAL LEFT JOIN (
    SELECT
        'tb' AS platform,
        '5cac112e98ef4100118a9c9f' AS shop_id,
        '方太官方旗舰店' AS shop_name
    FROM numbers(1)
) AS shop_info
USING(platform, shop_name)


-- ETL(jd)
INSERT INTO buffer.ft_dwd_transfer_msg_buffer
SELECT
    lower(hex(MD5(concat(shop_id, from_snick, to_snick, cnick, real_buyer_nick, create_time)))) AS id,
    day,
    platform,
    shop_id,
    shop_name,
    from_snick,
    to_snick,
    cnick,
    real_buyer_nick,
    create_time
FROM (
    SELECT
        day,
        platform,
        plat_user_id AS shop_name,
        from_spin AS from_snick,
        to_spin AS to_snick,
        buyer_one_id AS cnick,
        pin AS real_buyer_nick,
        create_time
    FROM ods.transfer_msg_all
    WHERE day BETWEEN 20220911 AND 20220918
    AND platform = 'jd'
    AND plat_user_id IN [
        '方太京东自营旗舰店',
        '方太京东旗舰店',
        '方太官方旗舰店',
        '方太厨卫旗舰店'
    ]
) AS transfer_msg_info
GLOBAL LEFT JOIN (
    SELECT
        'jd' AS platform,
        shop_id,
        shop_name
    FROM numbers(1)
    ARRAY JOIN
        [
            '5e9d390d68283c002457b52f',
            '5edfa47c8f591c00163ef7d6',
            '5e9d350bcff5ed002486ded8',
            '5eb8acf16119f0001cbdaa5f'
        ] AS shop_id,
        [
            '方太京东自营旗舰店',
            '方太京东旗舰店',
            '方太官方旗舰店',
            '方太厨卫旗舰店'
        ] AS shop_name
) AS shop_info
USING(platform, shop_name)