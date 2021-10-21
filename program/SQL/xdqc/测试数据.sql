-- 告警表
CREATE TABLE tmp.event_alert_local ON CLUSTER cluster_3s_2r (
    `id` String,
    `level` Int64,
    `warning_type` String,
    `dialog_id` String,
    `message_id` String,
    `time` String,
    `day` Int64,
    `is_finished` String,
    `finish_time` String,
    `update_time` DateTime
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tmp/tables/{layer}_{shard}/event_alert_local',
    '{replica}'
)
ORDER BY (`dialog_id`,`id`)
SETTINGS index_granularity=8192, storage_policy='rr'

CREATE TABLE tmp.event_alert_all ON CLUSTER cluster_3s_2r
AS tmp.event_alert_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'event_alert_local', rand())

-- 填充数据
INSERT INTO tmp.event_alert_all
SELECT
    id, 
    type AS level,
    reason AS warning_type,
    dialog_id,
    '' AS message_id,
    create_time AS time,
    day,
    done as is_finished,
    now() as finished_time,
    now() as update_time
FROM xqc_ods.event_alert_all


-- 会话表
CREATE TABLE tmp.dialog_local ON CLUSTER cluster_3s_2r(
    `id` String,
    `platform` String,
    `shop_id` String,
    `seller_nick`  String,
    `snick` String,
    `cnick` String,
    `employee_name` String,
    `superior_name` String,
    `time` String,
    `hour` Int64,
    `day` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tmp/tables/{layer}_{shard}/dialog_local',
    '{replica}'
)
PARTITION BY (`day`, `platform`)
ORDER BY `snick`
SETTINGS index_granularity=8192, storage_policy='rr'

CREATE TABLE tmp.dialog_all ON CLUSTER cluster_3s_2r 
AS tmp.dialog_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'dialog_local', rand())

-- 填充数据
INSERT INTO tmp.dialog_all
SELECT
  dialog_id AS id,
  platform,
  shop_id,
  seller_nick,
  snick,
  cnick,
  '' AS employee_name,
  '' AS superior_name,
  update_time AS time,
  toHour(parseDateTimeBestEffort(update_time)) AS hour,
  day
FROM xqc_ods.event_alert_all
WHERE day=20210915

INSERT INTO xqc_ods.dialog_all
SELECT
  dialog_id AS id,
  platform,
  shop_id,
  seller_nick,
  snick,
  cnick,
  '' AS employee_name,
  '' AS superior_name,
  update_time AS time,
  toHour(parseDateTimeBestEffort(update_time)) AS hour,
  day
FROM xqc_ods.event_alert_all
WHERE day BETWEEN 20210814 AND 20210914

-- 导出数据

docker exec -i 96 clickhouse-client \
--port=19000 --query="select * from xqc_dim.group_all FORMAT Parquet" > /tmp/group.parq

docker exec -i 96 clickhouse-client \
--port=19000 --query="select * from xqc_dim.snick_all FORMAT Parquet" > /tmp/snick.parq

docker exec -i 9e clickhouse-client \
--port=19000 --query="INSERT INTO xqc_dim.group_all FORMAT Parquet" < /tmp/group.parq

docker exec -i 9e clickhouse-client \
--port=19000 --query="INSERT INTO xqc_dim.snick_all FORMAT Parquet" < /tmp/snick.parq

docker exec -i 9e clickhouse-client \
--port=19000 --query="INSERT INTO xqc_dim.group_all FORMAT Arrow" < /tmp/group.arrow
docker exec -i 9e clickhouse-client \
--port=19000 --query="INSERT INTO xqc_dim.snick_all FORMAT Arrow" < /tmp/snick.arrow



-- 测试company_id
5f73e9c1684bf70001413636
-- 方太测试数据
ALTER TABLE xqc_dim.group_local ON CLUSTER cluster_3s_2r DELETE WHERE company_id='5f747ba42c90fd0001254404'
INSERT INTO xqc_dim.group_all
VALUES 
('5f747ba42c90fd0001254404','方太','','','5f747ba42c90fd0001254401','一级部门1','False','',[],1),
('5f747ba42c90fd0001254404','方太','','','5f747ba42c90fd0001254402','一级部门2','False','',[],1),
('5f747ba42c90fd0001254404','方太','','','5f747ba42c90fd0001254403','一级部门3','False','',[],1),
('5f747ba42c90fd0001254404','方太','','','5edfa47c8f591c00163ef7d6','方太京东旗舰店','True','jd',['5f747ba42c90fd0001254401'],2),
('5f747ba42c90fd0001254404','方太','','','5e9d390d68283c002457b52f','方太京东自营旗舰店','True','jd',['5f747ba42c90fd0001254402'],2)
('5f747ba42c90fd0001254404','方太','','','5e9d350bcff5ed002486ded8','方太官方旗舰店','True','jd',['5f747ba42c90fd0001254403'],2)

`company_id`
5f747ba42c90fd0001254404

`shop_id`
5edfa47c8f591c00163ef7d6
5e9d390d68283c002457b52f
5e9d350bcff5ed002486ded8
5eb8acf16119f0001cbdaa5f
5cac112e98ef4100118a9c9f


-- 创建ClickHouse数据结构表
CREATE TABLE test.data_type_local ON CLUSTER cluster_3s_2r(
    id String,
    date_time DateTime
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/test/tables/{layer}_{shard}/data_type_local',
    '{replica}'
)
ORDER BY `id`
SETTINGS index_granularity=8192, storage_policy='default_policy'

CREATE TABLE test.data_type_all ON CLUSTER cluster_3s_2r
AS test.data_type_local
ENGINE = Distributed('cluster_3s_2r', 'test', 'data_type_local', rand())

