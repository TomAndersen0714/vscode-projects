CREATE TABLE tmp.client_arr (
    `m` String, 
    `spin` String,
    `ts` Array(Int32))
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tmp/tables/{layer}_{shard}/client_arr',
    '{replica}'
)
ORDER BY (spin, m) SETTINGS storage_policy = 'rr',
    index_granularity = 8192