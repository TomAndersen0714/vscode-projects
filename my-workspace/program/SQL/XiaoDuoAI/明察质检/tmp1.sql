-- DROP TABLE tmp.ft_dwd_order_detail_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.ft_dwd_order_detail_local ON CLUSTER cluster_3s_2r
AS ft_dwd.order_detail_all
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (order_id, status, goods_id)
SETTINGS storage_policy = 'rr', index_granularity = 8192

-- DROP TABLE tmp.ft_dwd_order_detail_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.ft_dwd_order_detail_all ON CLUSTER cluster_3s_2r
AS tmp.ft_dwd_order_detail_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'ft_dwd_order_detail_local', rand())


docker exec -i 9043cb24167c clickhouse-client --port=29000 --query \
"INSERT INTO tmp.ft_dwd_order_detail_all FORMAT Parquet" \
< /opt/bigdata/ft_dwd.order_detail_all_20220101_20220926.parquet