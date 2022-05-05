-- 表结构同步
buffer.xdqc_dialog_buffer
ods.xdqc_dialog_local
ods.xdqc_dialog_all

buffer.xdqc_dialog_update_buffer
ods.xdqc_dialog_update_local
ods.xdqc_dialog_update_all

dwd.xdqc_dialog_local
dwd.xdqc_dialog_all

-- dwd.xdqc_dialog_all
-- PS: 为了便于开发, 故调整 dwd.xdqc_dialog_all 表列的位置
-- tb/mini/jd
ALTER TABLE dwd.xdqc_dialog_local ON CLUSTER cluster_3s_2r
MODIFY COLUMN IF EXISTS `wx_rule_stats_id` Array(String) AFTER `update_time`,
MODIFY COLUMN IF EXISTS `wx_rule_stats_score` Array(Int32) AFTER `wx_rule_stats_id`,
MODIFY COLUMN IF EXISTS `wx_rule_stats_count` Array(UInt32) AFTER `wx_rule_stats_score`,
MODIFY COLUMN IF EXISTS `wx_rule_add_stats_id` Array(String) AFTER `wx_rule_stats_count`,
MODIFY COLUMN IF EXISTS `wx_rule_add_stats_score` Array(Int32) AFTER `wx_rule_add_stats_id`,
MODIFY COLUMN IF EXISTS `wx_rule_add_stats_count` Array(UInt32) AFTER `wx_rule_add_stats_score`,
ADD COLUMN IF NOT EXISTS `abnormals_rule_id` Array(String) AFTER `abnormals_type`,
ADD COLUMN IF NOT EXISTS `abnormals_score` Array(Int32) AFTER `abnormals_rule_id`,
ADD COLUMN IF NOT EXISTS `excellents_rule_id` Array(String) AFTER `excellents_type`,
ADD COLUMN IF NOT EXISTS `excellents_score` Array(Int32) AFTER `excellents_rule_id`,
ADD COLUMN IF NOT EXISTS `s_emotion_rule_id` Array(Int32) AFTER `s_emotion_type`,
ADD COLUMN IF NOT EXISTS `s_emotion_score` Array(Int32) AFTER `s_emotion_rule_id`,
ADD COLUMN IF NOT EXISTS `c_emotion_rule_id` Array(Int32) AFTER `s_emotion_type`,
ADD COLUMN IF NOT EXISTS `c_emotion_score` Array(Int32) AFTER `c_emotion_rule_id`

ALTER TABLE dwd.xdqc_dialog_all ON CLUSTER cluster_3s_2r
MODIFY COLUMN IF EXISTS `wx_rule_stats_id` Array(String) AFTER `update_time`,
MODIFY COLUMN IF EXISTS `wx_rule_stats_score` Array(Int32) AFTER `wx_rule_stats_id`,
MODIFY COLUMN IF EXISTS `wx_rule_stats_count` Array(UInt32) AFTER `wx_rule_stats_score`,
MODIFY COLUMN IF EXISTS `wx_rule_add_stats_id` Array(String) AFTER `wx_rule_stats_count`,
MODIFY COLUMN IF EXISTS `wx_rule_add_stats_score` Array(Int32) AFTER `wx_rule_add_stats_id`,
MODIFY COLUMN IF EXISTS `wx_rule_add_stats_count` Array(UInt32) AFTER `wx_rule_add_stats_score`,
ADD COLUMN IF NOT EXISTS `abnormals_rule_id` Array(String) AFTER `abnormals_type`,
ADD COLUMN IF NOT EXISTS `abnormals_score` Array(Int32) AFTER `abnormals_rule_id`,
ADD COLUMN IF NOT EXISTS `excellents_rule_id` Array(String) AFTER `excellents_type`,
ADD COLUMN IF NOT EXISTS `excellents_score` Array(Int32) AFTER `excellents_rule_id`,
ADD COLUMN IF NOT EXISTS `s_emotion_rule_id` Array(Int32) AFTER `s_emotion_type`,
ADD COLUMN IF NOT EXISTS `s_emotion_score` Array(Int32) AFTER `s_emotion_rule_id`,
ADD COLUMN IF NOT EXISTS `c_emotion_rule_id` Array(Int32) AFTER `s_emotion_type`,
ADD COLUMN IF NOT EXISTS `c_emotion_score` Array(Int32) AFTER `c_emotion_rule_id`

-- dy/ks/pdd
ALTER TABLE dwd.xdqc_dialog_local ON CLUSTER cluster_3s_2r
DROP COLUMN IF EXISTS `wx_rule_stats_id`,
DROP COLUMN IF EXISTS `wx_rule_stats_score`,
DROP COLUMN IF EXISTS `wx_rule_stats_count`,
DROP COLUMN IF EXISTS `wx_rule_add_stats_id`,
DROP COLUMN IF EXISTS `wx_rule_add_stats_score`,
DROP COLUMN IF EXISTS `wx_rule_add_stats_count`
SETTINGS replication_alter_partitions_sync=2

ALTER TABLE dwd.xdqc_dialog_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `wx_rule_stats_id` Array(String) AFTER `update_time`,
ADD COLUMN IF NOT EXISTS `wx_rule_stats_score` Array(Int32) AFTER `wx_rule_stats_id`,
ADD COLUMN IF NOT EXISTS `wx_rule_stats_count` Array(UInt32) AFTER `wx_rule_stats_score`,
ADD COLUMN IF NOT EXISTS `wx_rule_add_stats_id` Array(String) AFTER `wx_rule_stats_count`,
ADD COLUMN IF NOT EXISTS `wx_rule_add_stats_score` Array(Int32) AFTER `wx_rule_add_stats_id`,
ADD COLUMN IF NOT EXISTS `wx_rule_add_stats_count` Array(UInt32) AFTER `wx_rule_add_stats_score`,
ADD COLUMN IF NOT EXISTS `abnormals_rule_id` Array(String) AFTER `abnormals_type`,
ADD COLUMN IF NOT EXISTS `abnormals_score` Array(Int32) AFTER `abnormals_rule_id`,
ADD COLUMN IF NOT EXISTS `excellents_rule_id` Array(String) AFTER `excellents_type`,
ADD COLUMN IF NOT EXISTS `excellents_score` Array(Int32) AFTER `excellents_rule_id`,
ADD COLUMN IF NOT EXISTS `s_emotion_rule_id` Array(Int32) AFTER `s_emotion_type`,
ADD COLUMN IF NOT EXISTS `s_emotion_score` Array(Int32) AFTER `s_emotion_rule_id`,
ADD COLUMN IF NOT EXISTS `c_emotion_rule_id` Array(Int32) AFTER `s_emotion_type`,
ADD COLUMN IF NOT EXISTS `c_emotion_score` Array(Int32) AFTER `c_emotion_rule_id`
SETTINGS replication_alter_partitions_sync=2

DROP TABLE dwd.xdqc_dialog_all ON CLUSTER cluster_3s_2r SYNC
CREATE TABLE dwd.xdqc_dialog_all ON CLUSTER cluster_3s_2r
AS dwd.xdqc_dialog_local
ENGINE = Distributed('cluster_3s_2r', 'dwd', 'xdqc_dialog_local', xxHash64(platform, channel, seller_nick, _id))


-- ods.xdqc_dialog_all
DROP TABLE ods.xdqc_dialog_local ON CLUSTER cluster_3s_2r SYNC
CREATE TABLE ods.xdqc_dialog_local ON CLUSTER cluster_3s_2r
AS dwd.xdqc_dialog_all
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/tables/{layer}_{shard}/{table}', '{replica}')
PARTITION BY toYYYYMMDD(begin_time)
ORDER BY (platform, seller_nick, _id)
SETTINGS storage_policy = 'rr', index_granularity = 8192

ALTER TABLE ods.xdqc_dialog_local ON CLUSTER cluster_3s_2r
DROP COLUMN `sign`
SETTINGS replication_alter_partitions_sync=2

DROP TABLE ods.xdqc_dialog_all ON CLUSTER cluster_3s_2r SYNC
CREATE TABLE ods.xdqc_dialog_all ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_local
ENGINE = Distributed('cluster_3s_2r', 'ods', 'xdqc_dialog_local', rand())

DROP TABLE buffer.xdqc_dialog_buffer ON CLUSTER cluster_3s_2r SYNC
CREATE TABLE buffer.xdqc_dialog_buffer ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_all
ENGINE = Buffer('ods', 'xdqc_dialog_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

-- ods.xdqc_dialog_update_all
DROP TABLE ods.xdqc_dialog_update_local ON CLUSTER cluster_3s_2r SYNC
CREATE TABLE ods.xdqc_dialog_update_local ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_all
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/tables/{layer}_{shard}/{table}', '{replica}')
PARTITION BY toYYYYMMDD(begin_time)
ORDER BY (platform, seller_nick, _id)
SETTINGS storage_policy = 'rr', index_granularity = 8192

DROP TABLE ods.xdqc_dialog_update_all ON CLUSTER cluster_3s_2r SYNC
CREATE TABLE ods.xdqc_dialog_update_all ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_update_local
ENGINE = Distributed('cluster_3s_2r', 'ods', 'xdqc_dialog_update_local', rand())


DROP TABLE buffer.xdqc_dialog_update_buffer ON CLUSTER cluster_3s_2r SYNC
CREATE TABLE buffer.xdqc_dialog_update_buffer ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_update_all
ENGINE = Buffer('ods', 'xdqc_dialog_update_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)