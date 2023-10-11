-- ALTER TABLE xqc_dws.snick_stat_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `valid_subtract_score_sum`;
ALTER TABLE xqc_dws.snick_stat_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `valid_subtract_score_sum` Int64 AFTER `add_score_sum`,
ADD COLUMN IF NOT EXISTS `valid_add_score_sum` Int64 AFTER `valid_subtract_score_sum`,
ADD COLUMN IF NOT EXISTS `valid_dialog_cnt` Int64 AFTER `dialog_cnt`;

-- ALTER TABLE xqc_dws.snick_stat_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `valid_subtract_score_sum`;
ALTER TABLE xqc_dws.snick_stat_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `valid_subtract_score_sum` Int64 AFTER `add_score_sum`,
ADD COLUMN IF NOT EXISTS `valid_add_score_sum` Int64 AFTER `valid_subtract_score_sum`,
ADD COLUMN IF NOT EXISTS `valid_dialog_cnt` Int64 AFTER `dialog_cnt`;