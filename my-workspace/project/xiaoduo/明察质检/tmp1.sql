ALTER TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r
DROP COLUMN IF EXISTS `mark_employee_id`,
DROP COLUMN IF EXISTS `mark_employee_name`


ALTER TABLE ods.xinghuan_dialog_tag_score_all ON CLUSTER cluster_3s_2r
DROP COLUMN IF EXISTS `mark_employee_id`,
DROP COLUMN IF EXISTS `mark_employee_name`