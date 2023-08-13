ADD COLUMN IF NOT EXISTS `custom_tagged_subtract_score_dialog_cnt` Int64 AFTER `custom_tagged_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `custom_tagged_add_score_dialog_cnt` Int64 AFTER `custom_tagged_subtract_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `custom_tagged_zero_score_dialog_cnt` Int64 AFTER `custom_tagged_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `manual_tagged_subtract_score_dialog_cnt` Int64 AFTER `manual_tagged_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `manual_tagged_add_score_dialog_cnt` Int64 AFTER `manual_tagged_subtract_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `manual_tagged_zero_score_dialog_cnt` Int64 AFTER `manual_tagged_add_score_dialog_cnt`



-- ADD COLUMN IF NOT EXISTS `custom_tagged_subtract_score_dialog_cnt` Int64 AFTER `custom_tagged_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `custom_tagged_add_score_dialog_cnt` Int64 AFTER `custom_tagged_subtract_score_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `custom_tagged_zero_score_dialog_cnt` Int64 AFTER `custom_tagged_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `custom_zero_score_tagged_dialog_cnt` Int64 AFTER `custom_add_score_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `manual_tagged_subtract_score_dialog_cnt` Int64 AFTER `manual_tagged_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `manual_tagged_add_score_dialog_cnt` Int64 AFTER `manual_tagged_subtract_score_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `manual_tagged_zero_score_dialog_cnt` Int64 AFTER `manual_tagged_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `manual_zero_score_tagged_dialog_cnt` Int64 AFTER `manual_add_score_dialog_cnt`,