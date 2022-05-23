/*
ods.xinghuan_dialog_tag_score_local
ods.xinghuan_dialog_tag_score_all
*/
DROP TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r NO DELAY
DROP TABLE ods.xinghuan_dialog_tag_score_all ON CLUSTER cluster_3s_2r NO DELAY


CREATE TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r (
    `seller_nick` String,
    `platform` String,
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

/*
buffer.xdqc_dialog_update_buffer
ods.xdqc_dialog_update_all
ods.xdqc_dialog_update_local
buffer.xdqc_dialog_buffer
ods.xdqc_dialog_all
ods.xdqc_dialog_local
dwd.xdqc_dialog_all
dwd.xdqc_dialog_local
*/

-- ods.xdqc_dialog_local
ALTER TABLE ods.xdqc_dialog_local ON CLUSTER cluster_3s_2r
ADD COLUMN tag_score_stats.count Array(UInt32) AFTER `tag_score_stats.score`,
ADD COLUMN tag_score_stats.md Array(UInt8) AFTER `tag_score_stats.count`,
ADD COLUMN tag_score_stats.mm Array(UInt8) AFTER `tag_score_stats.md`,
ADD COLUMN tag_score_add_stats.count Array(UInt32) AFTER `tag_score_add_stats.score`,
ADD COLUMN tag_score_add_stats.md Array(UInt8) AFTER `tag_score_add_stats.count`,
ADD COLUMN tag_score_add_stats.mm Array(UInt8) AFTER `tag_score_add_stats.md`
-- ods.xdqc_dialog_all
ALTER TABLE ods.xdqc_dialog_all ON CLUSTER cluster_3s_2r
ADD COLUMN tag_score_stats.count Array(UInt32) AFTER `tag_score_stats.score`,
ADD COLUMN tag_score_stats.md Array(UInt8) AFTER `tag_score_stats.count`,
ADD COLUMN tag_score_stats.mm Array(UInt8) AFTER `tag_score_stats.md`,
ADD COLUMN tag_score_add_stats.count Array(UInt32) AFTER `tag_score_add_stats.score`,
ADD COLUMN tag_score_add_stats.md Array(UInt8) AFTER `tag_score_add_stats.count`,
ADD COLUMN tag_score_add_stats.mm Array(UInt8) AFTER `tag_score_add_stats.md`
-- buffer.xdqc_dialog_buffer
DROP TABLE buffer.xdqc_dialog_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xdqc_dialog_buffer ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_all
ENGINE = Buffer('ods', 'xdqc_dialog_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

-- ods.xdqc_dialog_update_local
ALTER TABLE ods.xdqc_dialog_update_local ON CLUSTER cluster_3s_2r
ADD COLUMN tag_score_stats.count Array(UInt32) AFTER `tag_score_stats.score`,
ADD COLUMN tag_score_stats.md Array(UInt8) AFTER `tag_score_stats.count`,
ADD COLUMN tag_score_stats.mm Array(UInt8) AFTER `tag_score_stats.md`,
ADD COLUMN tag_score_add_stats.count Array(UInt32) AFTER `tag_score_add_stats.score`,
ADD COLUMN tag_score_add_stats.md Array(UInt8) AFTER `tag_score_add_stats.count`,
ADD COLUMN tag_score_add_stats.mm Array(UInt8) AFTER `tag_score_add_stats.md`
-- ods.xdqc_dialog_update_all
ALTER TABLE ods.xdqc_dialog_update_all ON CLUSTER cluster_3s_2r
ADD COLUMN tag_score_stats.count Array(UInt32) AFTER `tag_score_stats.score`,
ADD COLUMN tag_score_stats.md Array(UInt8) AFTER `tag_score_stats.count`,
ADD COLUMN tag_score_stats.mm Array(UInt8) AFTER `tag_score_stats.md`,
ADD COLUMN tag_score_add_stats.count Array(UInt32) AFTER `tag_score_add_stats.score`,
ADD COLUMN tag_score_add_stats.md Array(UInt8) AFTER `tag_score_add_stats.count`,
ADD COLUMN tag_score_add_stats.mm Array(UInt8) AFTER `tag_score_add_stats.md`
-- buffer.xdqc_dialog_update_buffer
DROP TABLE buffer.xdqc_dialog_update_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xdqc_dialog_update_buffer ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_update_all
ENGINE = Buffer('ods', 'xdqc_dialog_update_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

-- dwd.xdqc_dialog_local
ALTER TABLE dwd.xdqc_dialog_local ON CLUSTER cluster_3s_2r
ADD COLUMN tag_score_stats_count Array(UInt32) AFTER `tag_score_stats_score`,
ADD COLUMN tag_score_stats_md Array(UInt8) AFTER `tag_score_stats_count`,
ADD COLUMN tag_score_stats_mm Array(UInt8) AFTER `tag_score_stats_md`,
ADD COLUMN tag_score_add_stats_count Array(UInt32) AFTER `tag_score_add_stats_score`,
ADD COLUMN tag_score_add_stats_md Array(UInt8) AFTER `tag_score_add_stats_count`,
ADD COLUMN tag_score_add_stats_mm Array(UInt8) AFTER `tag_score_add_stats_md`
-- dwd.xdqc_dialog_all
ALTER TABLE dwd.xdqc_dialog_all ON CLUSTER cluster_3s_2r
ADD COLUMN tag_score_stats_count Array(UInt32) AFTER `tag_score_stats_score`,
ADD COLUMN tag_score_stats_md Array(UInt8) AFTER `tag_score_stats_count`,
ADD COLUMN tag_score_stats_mm Array(UInt8) AFTER `tag_score_stats_md`,
ADD COLUMN tag_score_add_stats_count Array(UInt32) AFTER `tag_score_add_stats_score`,
ADD COLUMN tag_score_add_stats_md Array(UInt8) AFTER `tag_score_add_stats_count`,
ADD COLUMN tag_score_add_stats_mm Array(UInt8) AFTER `tag_score_add_stats_md`