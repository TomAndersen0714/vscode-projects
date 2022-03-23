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
-- PS: 表结构变更和Airflow Extract脚本变更必须同步执行, 且表结构要先行, 建议先在小平台试运行(如:pdd)

-- tb/mini/pdd/dy
-- ods.xdqc_dialog_local
ALTER TABLE ods.xdqc_dialog_local ON CLUSTER cluster_3s_2r
ADD COLUMN xrule_stats.id Array(String) AFTER `rule_add_stats.count`,
ADD COLUMN xrule_stats.score Array(Int32) AFTER `xrule_stats.id`,
ADD COLUMN xrule_stats.count Array(UInt32) AFTER `xrule_stats.score`
-- ods.xdqc_dialog_all
ALTER TABLE ods.xdqc_dialog_all ON CLUSTER cluster_3s_2r
ADD COLUMN xrule_stats.id Array(String) AFTER `rule_add_stats.count`,
ADD COLUMN xrule_stats.score Array(Int32) AFTER `xrule_stats.id`,
ADD COLUMN xrule_stats.count Array(UInt32) AFTER `xrule_stats.score`
-- buffer.xdqc_dialog_buffer
DROP TABLE buffer.xdqc_dialog_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xdqc_dialog_buffer ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_all
ENGINE = Buffer('ods', 'xdqc_dialog_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

-- ods.xdqc_dialog_update_local
ALTER TABLE ods.xdqc_dialog_update_local ON CLUSTER cluster_3s_2r
ADD COLUMN xrule_stats.id Array(String) AFTER `rule_add_stats.count`,
ADD COLUMN xrule_stats.score Array(Int32) AFTER `xrule_stats.id`,
ADD COLUMN xrule_stats.count Array(UInt32) AFTER `xrule_stats.score`
-- ods.xdqc_dialog_update_all
ALTER TABLE ods.xdqc_dialog_update_all ON CLUSTER cluster_3s_2r
ADD COLUMN xrule_stats.id Array(String) AFTER `rule_add_stats.count`,
ADD COLUMN xrule_stats.score Array(Int32) AFTER `xrule_stats.id`,
ADD COLUMN xrule_stats.count Array(UInt32) AFTER `xrule_stats.score`
-- buffer.xdqc_dialog_update_buffer
DROP TABLE buffer.xdqc_dialog_update_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xdqc_dialog_update_buffer ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_update_all
ENGINE = Buffer('ods', 'xdqc_dialog_update_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

-- dwd.xdqc_dialog_local
ALTER TABLE dwd.xdqc_dialog_local ON CLUSTER cluster_3s_2r
ADD COLUMN xrule_stats_id Array(String) AFTER `rule_add_stats_count`,
ADD COLUMN xrule_stats_score Array(Int32) AFTER `xrule_stats_id`,
ADD COLUMN xrule_stats_count Array(UInt32) AFTER `xrule_stats_score`
-- dwd.xdqc_dialog_all
ALTER TABLE dwd.xdqc_dialog_all ON CLUSTER cluster_3s_2r
ADD COLUMN xrule_stats_id Array(String) AFTER `rule_add_stats_count`,
ADD COLUMN xrule_stats_score Array(Int32) AFTER `xrule_stats_id`,
ADD COLUMN xrule_stats_count Array(UInt32) AFTER `xrule_stats_score`

-- ks/jd
-- ods.xdqc_dialog_all
ALTER TABLE ods.xdqc_dialog_all
ADD COLUMN xrule_stats.id Array(String) AFTER `rule_add_stats.count`,
ADD COLUMN xrule_stats.score Array(Int32) AFTER `xrule_stats.id`,
ADD COLUMN xrule_stats.count Array(UInt32) AFTER `xrule_stats.score`
-- buffer.xdqc_dialog_buffer
DROP TABLE buffer.xdqc_dialog_buffer
CREATE TABLE buffer.xdqc_dialog_buffer
AS ods.xdqc_dialog_all
ENGINE = Buffer('ods', 'xdqc_dialog_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)
-- ods.xdqc_dialog_update_all
ALTER TABLE ods.xdqc_dialog_update_all
ADD COLUMN xrule_stats.id Array(String) AFTER `rule_add_stats.count`,
ADD COLUMN xrule_stats.score Array(Int32) AFTER `xrule_stats.id`,
ADD COLUMN xrule_stats.count Array(UInt32) AFTER `xrule_stats.score`
-- buffer.xdqc_dialog_update_buffer
DROP TABLE buffer.xdqc_dialog_update_buffer
CREATE TABLE buffer.xdqc_dialog_update_buffer
AS ods.xdqc_dialog_update_all
ENGINE = Buffer('ods', 'xdqc_dialog_update_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)
-- dwd.xdqc_dialog_all
ALTER TABLE dwd.xdqc_dialog_all
ADD COLUMN xrule_stats_id Array(String) AFTER `rule_add_stats_count`,
ADD COLUMN xrule_stats_score Array(Int32) AFTER `xrule_stats_id`,
ADD COLUMN xrule_stats_count Array(UInt32) AFTER `xrule_stats_score`


-- Airflow Dialog Extract脚本变更
-- 字符串替换
            rule_stats.id, rule_stats.score, rule_stats.count, rule_add_stats.id, rule_add_stats.score, rule_add_stats.count,
            score, score_add, question_count, answer_count, first_answer_time, qa_time_sum, qa_round_sum, focus_goods_id,

            rule_stats.id, rule_stats.score, rule_stats.count, rule_add_stats.id, rule_add_stats.score, rule_add_stats.count,
            xrule_stats.id, xrule_stats.score, xrule_stats.count, 
            score, score_add, question_count, answer_count, first_answer_time, qa_time_sum, qa_round_sum, focus_goods_id,

-- 字符串替换
            rule_stats_id, rule_stats_score, rule_stats_count, rule_add_stats_id, rule_add_stats_score, rule_add_stats_count,
            score, score_add, question_count, answer_count, first_answer_time, qa_time_sum, qa_round_sum, focus_goods_id,

            rule_stats_id, rule_stats_score, rule_stats_count, rule_add_stats_id, rule_add_stats_score, rule_add_stats_count,
            xrule_stats_id, xrule_stats_score, xrule_stats_count, 
            score, score_add, question_count, answer_count, first_answer_time, qa_time_sum, qa_round_sum, focus_goods_id,
