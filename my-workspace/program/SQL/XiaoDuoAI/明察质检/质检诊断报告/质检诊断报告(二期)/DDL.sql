-- xqc_dws.tag_stat_all
ALTER TABLE xqc_dws.tag_stat_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `tag_dialog_cnt` Int64 AFTER `tag_score_sum`,
ADD COLUMN IF NOT EXISTS `tag_manual_dialog_cnt` Int64 AFTER `tag_dialog_cnt`;

ALTER TABLE xqc_dws.tag_stat_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `tag_dialog_cnt` Int64 AFTER `tag_score_sum`,
ADD COLUMN IF NOT EXISTS `tag_manual_dialog_cnt` Int64 AFTER `tag_dialog_cnt`;


-- xqc_dws.tag_group_stat_all
ALTER TABLE xqc_dws.tag_group_stat_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `subtract_score_manual_dialog_cnt` Int64 AFTER `subtract_score_dialog_cnt`;

ALTER TABLE xqc_dws.tag_group_stat_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `subtract_score_manual_dialog_cnt` Int64 AFTER `subtract_score_dialog_cnt`;