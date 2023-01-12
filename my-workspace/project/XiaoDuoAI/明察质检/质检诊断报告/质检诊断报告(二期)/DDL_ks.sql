-- xqc_dws.tag_stat_all


ALTER TABLE xqc_dws.tag_stat_local
ADD COLUMN IF NOT EXISTS `tag_dialog_cnt` Int64 AFTER `tag_score_sum`,
ADD COLUMN IF NOT EXISTS `tag_manual_dialog_cnt` Int64 AFTER `tag_dialog_cnt`;


ALTER TABLE xqc_dws.tag_stat_all
ADD COLUMN IF NOT EXISTS `tag_dialog_cnt` Int64 AFTER `tag_score_sum`,
ADD COLUMN IF NOT EXISTS `tag_manual_dialog_cnt` Int64 AFTER `tag_dialog_cnt`;


-- xqc_dws.tag_group_stat_all
ALTER TABLE xqc_dws.tag_group_stat_local
ADD COLUMN IF NOT EXISTS `subtract_score_manual_dialog_cnt` Int64 AFTER `subtract_score_dialog_cnt`;

ALTER TABLE xqc_dws.tag_group_stat_all
ADD COLUMN IF NOT EXISTS `subtract_score_manual_dialog_cnt` Int64 AFTER `subtract_score_dialog_cnt`;


ALTER TABLE xqc_dws.snick_stat_local
ADD COLUMN IF NOT EXISTS `tagged_dialog_cnt` Int64 AFTER `dialog_cnt`,
ADD COLUMN IF NOT EXISTS `ai_tagged_dialog_cnt` Int64 AFTER `tagged_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `custom_tagged_dialog_cnt` Int64 AFTER `ai_tagged_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `manual_tagged_dialog_cnt` Int64 AFTER `custom_tagged_dialog_cnt`;


ALTER TABLE xqc_dws.snick_stat_all
ADD COLUMN IF NOT EXISTS `tagged_dialog_cnt` Int64 AFTER `dialog_cnt`,
ADD COLUMN IF NOT EXISTS `ai_tagged_dialog_cnt` Int64 AFTER `tagged_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `custom_tagged_dialog_cnt` Int64 AFTER `ai_tagged_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `manual_tagged_dialog_cnt` Int64 AFTER `custom_tagged_dialog_cnt`;


