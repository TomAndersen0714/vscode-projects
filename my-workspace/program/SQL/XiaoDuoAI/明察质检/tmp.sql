CREATE TABLE cdp_dim.tag_group_local on cluster cluster_3s_2r(
    `platform` String,
    `shop_id` String, 
    `group_id` String,
    `group_name` String,
    `is_system` Int8,
    `is_deleted` Int8,
    `create_time` Int64, 
    `update_time` Int64 
    ) 
ENGINE = ReplicatedMergeTree('/clickhouse/cdp_dim/tables/{layer}_{shard}/tag_group_local', '{replica}') 
ORDER BY (platform, shop_id) SETTINGS index_granularity = 8192