-- 暂停 Airflow 脚本

-- 修改 Airflow 脚本, 更换为白名单版本

-- 删除Buffer表
DROP TABLE buffer.xdqc_dialog_buffer ON CLUSTER cluster_3s_2r
DROP TABLE buffer.xdqc_dialog_update_buffer ON CLUSTER cluster_3s_2r

-- 修改ods表结构
ALTER TABLE ods.xdqc_dialog_local ON CLUSTER cluster_3s_2r
ADD COLUMN wx_rule_stats.id Array(String),
ADD COLUMN wx_rule_stats.score Array(Int32),
ADD COLUMN wx_rule_stats.count Array(UInt32),
ADD COLUMN wx_rule_add_stats.id Array(String),
ADD COLUMN wx_rule_add_stats.score Array(Int32),
ADD COLUMN wx_rule_add_stats.count Array(UInt32)

ALTER TABLE ods.xdqc_dialog_all ON CLUSTER cluster_3s_2r
ADD COLUMN wx_rule_stats.id Array(String),
ADD COLUMN wx_rule_stats.score Array(Int32),
ADD COLUMN wx_rule_stats.count Array(UInt32),
ADD COLUMN wx_rule_add_stats.id Array(String),
ADD COLUMN wx_rule_add_stats.score Array(Int32),
ADD COLUMN wx_rule_add_stats.count Array(UInt32)

ALTER TABLE ods.xdqc_dialog_update_local ON CLUSTER cluster_3s_2r
ADD COLUMN wx_rule_stats.id Array(String),
ADD COLUMN wx_rule_stats.score Array(Int32),
ADD COLUMN wx_rule_stats.count Array(UInt32),
ADD COLUMN wx_rule_add_stats.id Array(String),
ADD COLUMN wx_rule_add_stats.score Array(Int32),
ADD COLUMN wx_rule_add_stats.count Array(UInt32)

ALTER TABLE ods.xdqc_dialog_update_all ON CLUSTER cluster_3s_2r
ADD COLUMN wx_rule_stats.id Array(String),
ADD COLUMN wx_rule_stats.score Array(Int32),
ADD COLUMN wx_rule_stats.count Array(UInt32),
ADD COLUMN wx_rule_add_stats.id Array(String),
ADD COLUMN wx_rule_add_stats.score Array(Int32),
ADD COLUMN wx_rule_add_stats.count Array(UInt32)


-- 重建Buffer表
CREATE TABLE buffer.xdqc_dialog_buffer ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_all
ENGINE = Buffer('ods', 'xdqc_dialog_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

CREATE TABLE buffer.xdqc_dialog_update_buffer ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_update_all
ENGINE = Buffer('ods', 'xdqc_dialog_update_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

-- 修改dwd表结构
ALTER TABLE dwd.xdqc_dialog_local ON CLUSTER cluster_3s_2r
ADD COLUMN wx_rule_stats_id Array(String),
ADD COLUMN wx_rule_stats_score Array(Int32),
ADD COLUMN wx_rule_stats_count Array(UInt32),
ADD COLUMN wx_rule_add_stats_id Array(String),
ADD COLUMN wx_rule_add_stats_score Array(Int32),
ADD COLUMN wx_rule_add_stats_count Array(UInt32)

ALTER TABLE dwd.xdqc_dialog_all ON CLUSTER cluster_3s_2r
ADD COLUMN wx_rule_stats_id Array(String),
ADD COLUMN wx_rule_stats_score Array(Int32),
ADD COLUMN wx_rule_stats_count Array(UInt32),
ADD COLUMN wx_rule_add_stats_id Array(String),
ADD COLUMN wx_rule_add_stats_score Array(Int32),
ADD COLUMN wx_rule_add_stats_count Array(UInt32)