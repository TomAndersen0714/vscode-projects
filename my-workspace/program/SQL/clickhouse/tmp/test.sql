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



-- 测试 ReplicatedReplacingMergeTree 和 Final 关键字的使用方式
-- 本地表
CREATE TABLE test.rrmt_test_local ON CLUSTER cluster_3s_2r (
  `id` String,
  `level` Int64,
  `warning_type` String,
  `day` UInt64,
  `info` String,
  `update_time` DateTime64(3)
) 
ENGINE = ReplicatedReplacingMergeTree(
  '/clickhouse/test/tables/{layer}_{shard}/rrmt_test_local',
  '{replica}',
  update_time
)
PARTITION BY day 
PRIMARY KEY (level, warning_type) 
ORDER BY (level, warning_type, id)
SETTINGS index_granularity = 8192,
storage_policy = 'default'
-- 分布式表(随机分片)
CREATE TABLE test.rrmt_test_all ON CLUSTER cluster_3s_2r
AS test.rrmt_test_local
ENGINE = Distributed('cluster_3s_2r', 'test', 'rrmt_test_local', rand())
-- 测试数据
INSERT INTO TABLE test.rrmt_test_all VALUES('101',1,'测试告警',20211010,'测试开始',now())
INSERT INTO TABLE test.rrmt_test_all VALUES('101',1,'测试告警',20211010,'测试结束',now())

-- 观察数据是否更新
SELECT * FROM test.rrmt_test_all
SELECT * FROM test.rrmt_test_all FINAL
-- 强制合并数据, 观察数据是否合并
OPTIMIZE TABLE test.rrmt_test_local ON CLUSTER cluster_3s_2r
SELECT * FROM test.rrmt_test_all
SELECT * FROM test.rrmt_test_all FINAL



-- 清空旧表和旧数据
TRUNCATE TABLE test.rrmt_test_local ON CLUSTER cluster_3s_2r
DROP TABLE test.rrmt_test_all ON CLUSTER cluster_3s_2r
-- 更换分布式表(按照排序键的Hash值进行分片)
CREATE TABLE test.rrmt_test_all ON CLUSTER cluster_3s_2r
AS test.rrmt_test_local
ENGINE = Distributed('cluster_3s_2r', 'test', 'rrmt_test_local', xxHash64(level, warning_type, id))
-- 测试数据
INSERT INTO TABLE test.rrmt_test_all VALUES('101',1,'测试告警',20211010,'测试开始',now())
INSERT INTO TABLE test.rrmt_test_all VALUES('101',1,'测试告警',20211010,'测试结束',now())

-- 观察数据是否更新
SELECT * FROM test.rrmt_test_all
SELECT * FROM test.rrmt_test_all FINAL
-- 强制合并数据, 观察数据是否合并
OPTIMIZE TABLE test.rrmt_test_local ON CLUSTER cluster_3s_2r
SELECT * FROM test.rrmt_test_all
SELECT * FROM test.rrmt_test_all FINAL



-- 测试ClickHouse Tuple的使用方式
CREATE TABLE test.data_type_test(
    `tuple_type` Tuple(String,UInt8),
    `array_tuple_type` Array(Tuple(String,UInt8))
)
ENGINE=Memory()

INSERT INTO TABLE test.data_type_test VALUES(('Tom',1),[('Tom',1),('Alise',2)])
INSERT INTO TABLE test.data_type_test VALUES(('Tom',2),[('Tom',2),('Alise',3)])


-- 测试 FINAL 查询 ReplacingMergeTree 是否会强制合并 Data Part
-- 1. 建表
CREATE TABLE test.rmt_test_local (
  `id` String,
  `level` Int64,
  `warning_type` String,
  `day` UInt64,
  `info` String,
  `update_time` DateTime64(3)
) 
ENGINE = ReplacingMergeTree(update_time)
PARTITION BY day
PRIMARY KEY (level, warning_type) 
ORDER BY (level, warning_type, id)
SETTINGS index_granularity = 8192,
storage_policy = 'default'
-- 2. 测试数据
INSERT INTO test.rmt_test_local VALUES('101',1,'测试',20211010,'FINAL测试',now())
INSERT INTO test.rmt_test_local VALUES('101',1,'测试',20211010,'FINAL测试',now())
INSERT INTO test.rmt_test_local VALUES('102',2,'测试',20211010,'FINAL测试',now())
-- 3. 测试查询
SELECT * FROM test.rmt_test_local
SELECT * FROM test.rmt_test_local FINAL



-- 测试 ClickHouse 二级索引性能(cdh2), 单节点
-- 测试数据
SELECT COUNT(1) FROM ods.order_event
WHERE day = 20200616
AND order_id = '596310980357124399'
-- 构建二级索引前查询性能

SELECT COUNT(1) FROM ods.order_event 
WHERE day = 20200616 AND order_id = '596310980357124399' -- Read 49393 rows


SELECT COUNT(1) FROM ods.order_event 
WHERE order_id = '596310980357124399' -- Read 2024696 rows

-- 添加minmax索引
ALTER TABLE ods.order_event ADD INDEX order_id_idx order_id TYPE minmax GRANULARITY 1
-- 重构单个分区索引
ALTER TABLE ods.order_event MATERIALIZE INDEX order_id_idx IN PARTITION 20200616
-- 构建单个分区索引后查询性能
SELECT COUNT(1) FROM ods.order_event 
WHERE day = 20200616 AND order_id = '596310980357124399' -- Read 49393 rows
分析: 可能是由于分区已经是单个data part,而minmax索引的筛选粒度是data part,因此查询性能无变化

-- 重构全表的分区索引
ALTER TABLE ods.order_event MATERIALIZE INDEX order_id_idx
-- 构建全表索引后查询性能
SELECT COUNT(1) FROM ods.order_event 
WHERE order_id = '596310980357124399' -- Read 1977321 rows

-- 删除索引
ALTER TABLE ods.order_event DROP INDEX order_id_idx

-- 添加bloom_filter索引
ALTER TABLE ods.order_event ADD INDEX order_id_idx order_id TYPE bloom_filter GRANULARITY 1
-- 重构单个分区索引
ALTER TABLE ods.order_event MATERIALIZE INDEX order_id_idx IN PARTITION 20200616
-- 构建单个分区索引后查询性能
SELECT COUNT(1) FROM ods.order_event 
WHERE day = 20200616 AND order_id = '596310980357124399' -- Read 8192 rows
分析: bloom filter index 的过滤粒度是granules, 即 index_granularity 对应大小, 因此扫描结果是8192行

-- 重构全表的分区索引
ALTER TABLE ods.order_event MATERIALIZE INDEX order_id_idx
-- 构建全表索引后查询性能
SELECT COUNT(1) FROM ods.order_event 
WHERE order_id = '596310980357124399' -- Read 24406 rows

-- 删除索引
ALTER TABLE ods.order_event DROP INDEX order_id_idx




-- 给日志平台添加二级索引
ALTER TABLE xd_log.app_daily_local ON CLUSTER cluster_2s_1r
ADD INDEX trace_id_idx trace TYPE bloom_filter GRANULARITY 1
-- 重构索引
ALTER TABLE xd_log.app_daily_local ON CLUSTER cluster_2s_1r
MATERIALIZE INDEX order_id_idx IN PARTITION 20211012
-- 删除索引
ALTER TABLE xd_log.app_daily_local ON CLUSTER cluster_2s_1r
DROP INDEX trace_id_idx


-- 查询日志平台
SELECT COUNT(1) FROM xd_log.app_daily_all
WHERE toYYYYMMDD(ts)=20211012 
AND trace='046f281beaa93c5065d9fa48c7f18be9'


-- HDFS HA查询
CREATE TABLE test.ha_test(
    id String
)
STORED AS PARQUET
LOCATION 'hdfs://cdh0:8020/user/hive/warehouse/test.db/ha_test'


CREATE TABLE test.data_type_local
(
    `id` String,
    `tuple_type` Tuple(String, UInt8),
    `array_tuple_type` Array(Tuple(String, UInt8)),
    `date_time_type` DateTime(3)
)
ENGINE = MergeTree()
ORDER BY `id`
SETTINGS index_granularity = 8192,
storage_policy = 'default'