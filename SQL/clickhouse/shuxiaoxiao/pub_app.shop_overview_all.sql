DROP TABLE pub_app.shop_overview_local ON CLUSTER cluster_3s_2r
CREATE TABLE pub_app.shop_overview_local ON CLUSTER cluster_3s_2r(
    `platform` String,
    `shop_id` String,
    `served_pv` Int32,
    `served_uv` Int32,
    `received_pv` Int32,
    `received_uv` Int32,
    `created_uv` Int32,
    `created_order_cnt` Int32,
    `created_payment` Float64,
    `paid_uv` Int32,
    `paid_order_cnt` Int32,
    `paid_payment` Float64,
    `refund_uv` Int32,
    `refund_order_cnt` Int32,
    `refund_order_num` Int32,
    `refund_payment` Float64,
    `day` Int32
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/pub_app/tables/{layer}_{shard}/shop_overview_local',
    '{replica}'
) PARTITION BY day
ORDER BY (platform, shop_id) SETTINGS index_granularity = 8192;

DROP TABLE pub_app.shop_overview_all ON CLUSTER cluster_3s_2r
CREATE TABLE pub_app.shop_overview_all ON CLUSTER cluster_3s_2r (
    `platform` String,
    `shop_id` String,
    `served_pv` Int32,
    `served_uv` Int32,
    `received_pv` Int32,
    `received_uv` Int32,
    `created_uv` Int32,
    `created_order_cnt` Int32,
    `created_payment` Float64,
    `paid_uv` Int32,
    `paid_order_cnt` Int32,
    `paid_payment` Float64,
    `refund_uv` Int32,
    `refund_order_cnt` Int32,
    `refund_order_num` Int32,
    `refund_payment` Float64,
    `day` Int32
) ENGINE = Distributed(
    'cluster_3s_2r','pub_app','shop_overview_local',rand()
)

DROP TABLE buffer.pub_app_shop_overview_buffer ON CLUSTER cluster_3s_2r
CREATE TABLE buffer.pub_app_shop_overview_buffer ON CLUSTER cluster_3s_2r
AS pub_app.shop_overview_local
ENGINE = Buffer('pub_app', 'shop_overview_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

alter table pub_app.shop_overview_all on cluster cluster_3s_2r add column created_order_cnt Int32 after created_uv
alter table pub_app.shop_overview_local on cluster cluster_3s_2r add column created_order_cnt Int32 after created_uv