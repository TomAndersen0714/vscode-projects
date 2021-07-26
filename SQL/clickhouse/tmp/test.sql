CREATE TABLE tmp.drop_partition_test_local(
    `id` String,
    `year` Int32,
    `month` Int32,
    `day` Int32
)ENGINE = ReplicatedMergeTree(
    '/clickhouse/tmp/tables/{layer}_{shard}/drop_partition_test_local',
    '{replica}'
)
PARTITION BY (`year`,`month`,`day`)
ORDER BY (`year`,`month`,`day`)
SETTINGS index_granularity=8192,storage_policy='default'

INSERT INTO tmp.drop_partition_test_local VALUES (generateUUIDv4(),2021,7,13)

ALTER TABLE tmp.drop_partition_test_local DROP PARTITION 2021


-- 创建Buffer测试表
-- Create Buffer Test Table
DROP TABLE tmp.buffer_test_local ON CLUSTER cluster_3s_2r;
CREATE TABLE tmp.buffer_test_local ON CLUSTER cluster_3s_2r(
    `id` String,
    `year` Int32,
    `month` Int32,
    `day` Int32
)ENGINE = ReplicatedMergeTree(
    '/clickhouse/tmp/tables/{layer}_{shard}/buffer_test_local',
    '{replica}'
)
PARTITION BY (`year`,`month`)
ORDER BY (`day`)
SETTINGS index_granularity=8192,storage_policy='default';

DROP TABLE tmp.buffer_test_all ON CLUSTER cluster_3s_2r;
CREATE TABLE tmp.buffer_test_all ON CLUSTER cluster_3s_2r 
AS tmp.buffer_test_local 
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'buffer_test_local', rand());

DROP TABLE buffer.buffer_test_buffer ON CLUSTER cluster_3s_2r;
CREATE TABLE buffer.buffer_test_buffer ON CLUSTER cluster_3s_2r
AS tmp.buffer_test_all
ENGINE = Buffer('tmp', 'buffer_test_all', 16, 5, 10, 81920, 409600, 16777216, 67108864);

INSERT INTO tmp.buffer_test_all(`year`) values(2021)
INSERT INTO tmp.buffer_test_all values(generateUUIDv4(),2021,7,16)
TRUNCATE TABLE tmp.buffer_test_local ON CLUSTER cluster_3s_2r;
INSERT INTO buffer.buffer_test_buffer values(generateUUIDv4(),2021,7,16)

insert into xqc_ods.xdqc_tb_task_record_local 
values(generateUUIDv4(),'tb','tb','test','20210721','芸熙',generateUUIDv4(),100,6,6,0,generateUUIDv4())

select * from xqc_ods.xdqc_tb_task_record_local where `platform` = 'tb' and `date`=20210721