CREATE DATABASE IF NOT EXISTS dim ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- ALTER TABLE dim.question_b_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `question_sample`,
-- DROP COLUMN IF EXISTS `sid`

ALTER TABLE dim.question_b_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `question_sample` Array(String) AFTER `subcategory_ids`,
ADD COLUMN IF NOT EXISTS `sid` String AFTER `question_sample`;

-- ALTER TABLE dim.question_b_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `question_sample`,
-- DROP COLUMN IF EXISTS `sid`

ALTER TABLE dim.question_b_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `question_sample` Array(String) AFTER `subcategory_ids`,
ADD COLUMN IF NOT EXISTS `sid` String AFTER `question_sample`;