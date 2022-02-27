-- 老淘宝: 统计表添加platform字段, 并将对应的值设置为tb

-- ods.xinghuan_dialog_tag_score_all
ALTER TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r ADD COLUMN platform String FIRST
ALTER TABLE ods.xinghuan_dialog_tag_score_all ON CLUSTER cluster_3s_2r ADD COLUMN platform String FIRST

-- PS: ADD COLUMN...FIRST指令在v20.6及之后才支持:https://clickhouse.com/docs/en/whats-new/changelog/2020/#new-feature_6
-- 对于之前的ClickHouse, 无法控制列的顺序, 因此对于这种情况, 建议如果数据量不大的话, 备份+重建表格
-- https://github.com/ClickHouse/ClickHouse/blob/v20.4.2.9-stable/docs/en/sql_reference/statements/alter.md#alter_add-column

-- 手动设置默认值
ALTER TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r UPDATE platform='tb' WHERE 1=1

-- ods.qc_statistical_employee_all
ALTER TABLE ods.qc_statistical_employee_local ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER company_id
ALTER TABLE ods.qc_statistical_employee_all ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER company_id
-- 手动设置默认值
ALTER TABLE ods.qc_statistical_employee_local ON CLUSTER cluster_3s_2r UPDATE platform='tb' WHERE 1=1

-- ods.qc_statistical_department_all
ALTER TABLE ods.qc_statistical_department_local ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER company_id
ALTER TABLE ods.qc_statistical_department_all ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER company_id
-- 手动设置默认值
ALTER TABLE ods.qc_statistical_department_local ON CLUSTER cluster_3s_2r UPDATE platform='tb' WHERE 1=1

-- 注意: 修改表结构和修改任务必须同步进行


-- PS: 因为除淘宝外其他平台CH版本限制, 无法直接添加列到首行, 因此经过仔细考量, 最好的方案是老淘宝迁就其他平台, 即修改老淘宝的列顺序
-- 方案一
-- 1. 线下测试老淘宝CH是否支持更换列的位置
支持
-- 2. 如果1可行, 则先按照方案二备份数据, 然后更换老淘宝的列位置, 同时修改对应的Airflow任务
已备份
修改列位置
ALTER TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r
MODIFY COLUMN `seller_nick` String First

ALTER TABLE ods.xinghuan_dialog_tag_score_all ON CLUSTER cluster_3s_2r
MODIFY COLUMN `seller_nick` String First

-- 3. 将其他平台的对应表结构和任务向老淘宝对齐
ALTER TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER `seller_nick`
ALTER TABLE ods.xinghuan_dialog_tag_score_all ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER `seller_nick`

任务已修改, 但是表结构还未改动, 也未上线

-- 方案二
-- 1. 创建同类型表, 备份数据
CREATE TABLE ods.xinghuan_dialog_tag_score_local_bak ON CLUSTER cluster_3s_2r
AS ods.xinghuan_dialog_tag_score_local
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/tables/{layer}_{shard}/{table}', '{replica}')
PARTITION BY day
ORDER BY (day, seller_nick, group)
SETTINGS index_granularity = 8192, storage_policy = 'rr'

CREATE TABLE ods.xinghuan_dialog_tag_score_all_bak ON CLUSTER cluster_3s_2r
AS ods.xinghuan_dialog_tag_score_local_bak
ENGINE = Distributed('cluster_3s_2r', 'ods', 'xinghuan_dialog_tag_score_local_bak', rand())

-- 2. 备份数据
INSERT INTO ods.xinghuan_dialog_tag_score_all_bak
SELECT * 
FROM ods.xinghuan_dialog_tag_score_all
WHERE day <= 20210101

INSERT INTO ods.xinghuan_dialog_tag_score_all_bak
SELECT * 
FROM ods.xinghuan_dialog_tag_score_all
WHERE day > 20210101 
AND day <= 20210401

INSERT INTO ods.xinghuan_dialog_tag_score_all_bak
SELECT * 
FROM ods.xinghuan_dialog_tag_score_all
WHERE day > 20210401 
AND day <= 20210701

INSERT INTO ods.xinghuan_dialog_tag_score_all_bak
SELECT * 
FROM ods.xinghuan_dialog_tag_score_all
WHERE day > 20210701 


-- 核对数据量
SELECT COUNT(1) FROM ods.xinghuan_dialog_tag_score_all
SELECT COUNT(1) FROM ods.xinghuan_dialog_tag_score_all_bak
-- 3. 删除原表
DROP TABLE ods.xinghuan_dialog_tag_score_all ON CLUSTER cluster_3s_2r SYNC
DROP TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r SYNC
-- 4. 创建新表
CREATE TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r
(
    `platform` String,
    `seller_nick` String,
    `group` String,
    `snick` String,
    `dialog_id` String,
    `cnick` String,
    `tag_id` String,
    `name` String,
    `score` Int32,
    `cal_op` Int32,
    `day` Int32
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/tables/{layer}_{shard}/{table}', '{replica}')
PARTITION BY day
ORDER BY (day, seller_nick, group)
SETTINGS index_granularity = 8192, storage_policy = 'rr'

CREATE TABLE ods.xinghuan_dialog_tag_score_all ON CLUSTER cluster_3s_2r
AS ods.xinghuan_dialog_tag_score_local
ENGINE = Distributed('cluster_3s_2r', 'ods', 'xinghuan_dialog_tag_score_local', rand())
-- 5. 备份数据迁移回新表
INSERT INTO ods.xinghuan_dialog_tag_score_all
SELECT 'tb' AS platform, *
FROM ods.xinghuan_dialog_tag_score_all_bak
WHERE day <= 20211120

INSERT INTO ods.xinghuan_dialog_tag_score_all
SELECT 'tb' AS platform, *
FROM ods.xinghuan_dialog_tag_score_all_bak
WHERE day > 20211120