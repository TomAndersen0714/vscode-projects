-- 方案一: 路径枚举表
xqc_dim.company_department_all:
  department_id
  department_name
  is_shop
  parent_department_path
  level
  company_id
  company_name
  create_time
  update_time

-- 一级部门
BG:
SELECT department_name AS BG
FROM xqc_dim.company_department_all
WHERE company_id = '61372098699003a721a63a51' AND level = 1
-- 二级部门
BU:
SELECT department_id, department_name
FROM xqc_dim.company_department_all
WHERE company_id = '61372098699003a721a63a51' AND level = 2
-- 查询一级部门和snick的映射
SELECT department_id, department_name, snick
FROM xqc_dim.company_department_all
LEFT JOIN xqc_dim.snick
USING company_id
WHERE company_id = '61372098699003a721a63a51'
AND level = 1

-- 方案二: 关系枚举表
xqc_dim.company_department_all:
  department_id
  department_name
  is_shop
  parent_department
  level
  company_id
  company_name
  create_time
  update_time
-- 一级部门
SELECT distinct department_id, department_name
FROM xqc_dim.company_department_all
WHERE level=1

-- 二级部门
SELECT distinct department_id, department_name
FROM xqc_dim.company_department_all
WHERE level=2



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



SELECT distinct snick
FROM xqc_dim.snick_all
WHERE shop_id IN ({{shop_ids=shop_list}})