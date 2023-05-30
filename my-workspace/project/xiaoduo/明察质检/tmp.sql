ALTER TABLE dim.question_b_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `question_sample` Array(String) AFTER `subcategory_ids`,
ADD COLUMN IF NOT EXISTS `sid` String AFTER `question_sample`
SETTINGS database_atomic_wait_for_drop_and_detach_synchronously = 1;

ALTER TABLE dim.question_b_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `question_sample` Array(String) AFTER `subcategory_ids`,
ADD COLUMN IF NOT EXISTS `sid` String AFTER `question_sample`
SETTINGS database_atomic_wait_for_drop_and_detach_synchronously = 1;
