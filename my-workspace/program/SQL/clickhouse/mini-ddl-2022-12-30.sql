-- ft_dwd.session_msg_detail_all
-- ALTER TABLE ft_dwd.session_msg_detail_local ON CLUSTER cluster_3s_2r DROP COLUMN IF EXISTS `send_msg_from`
ALTER TABLE ft_dwd.session_msg_detail_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `send_msg_from` Int32 AFTER `is_first_msg_within_session`

-- ALTER TABLE ft_dwd.session_msg_detail_all ON CLUSTER cluster_3s_2r DROP COLUMN IF EXISTS `send_msg_from`
ALTER TABLE ft_dwd.session_msg_detail_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `send_msg_from` Int32 AFTER `is_first_msg_within_session`