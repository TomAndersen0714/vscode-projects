CREATE TABLE dim.category_subcategory_local ON CLUSTER cluster_3s_2r
(
  `_id` String,
  `category_id` String,
  `subcategory_id` String,
  `create_time` String
) ENGINE = ReplicatedMergeTree(
  '/clickhouse/dim/tables/{layer}_{shard}/category_subcategory_local',
  '{replica}'
)
ORDER BY category_id SETTINGS storage_policy = 'rr',
  index_granularity = 8192;


CREATE TABLE IF NOT EXISTS dim.category_subcategory_all ON CLUSTER cluster_3s_2r
AS dim.category_subcategory_local
ENGINE = Distributed('cluster_3s_2r', 'dim', 'category_subcategory_local', rand());