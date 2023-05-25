-- ft_dwd.session_msg_detail_all
-- ALTER TABLE ft_dwd.session_msg_detail_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `send_msg_from` NO DELAY
ALTER TABLE ft_dwd.session_msg_detail_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `send_msg_from` Int64 AFTER `is_first_msg_within_session`;

-- ALTER TABLE ft_dwd.session_msg_detail_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `send_msg_from` NO DELAY
ALTER TABLE ft_dwd.session_msg_detail_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `send_msg_from` Int64 AFTER `is_first_msg_within_session`;

-- ft_dwd.session_detail_all
ALTER TABLE ft_dwd.session_detail_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `m_session_send_cnt` Int64 AFTER `session_send_cnt`,
ADD COLUMN IF NOT EXISTS `qa_cnt` Int64 AFTER `m_session_send_cnt`,
ADD COLUMN IF NOT EXISTS `qa_reply_intervals_secs` Array(Int64) AFTER `qa_cnt`,
ADD COLUMN IF NOT EXISTS `m_qa_cnt` Int64 AFTER `qa_reply_intervals_secs`,
ADD COLUMN IF NOT EXISTS `m_qa_reply_intervals_secs` Array(Int64) AFTER `m_qa_cnt`;

ALTER TABLE ft_dwd.session_detail_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `m_session_send_cnt` Int64 AFTER `session_send_cnt`,
ADD COLUMN IF NOT EXISTS `qa_cnt` Int64 AFTER `m_session_send_cnt`,
ADD COLUMN IF NOT EXISTS `qa_reply_intervals_secs` Array(Int64) AFTER `qa_cnt`,
ADD COLUMN IF NOT EXISTS `m_qa_cnt` Int64 AFTER `qa_reply_intervals_secs`,
ADD COLUMN IF NOT EXISTS `m_qa_reply_intervals_secs` Array(Int64) AFTER `m_qa_cnt`;

ALTER TABLE buffer.ft_dwd_session_detail_buffer ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `m_session_send_cnt` Int64 AFTER `session_send_cnt`,
ADD COLUMN IF NOT EXISTS `qa_cnt` Int64 AFTER `m_session_send_cnt`,
ADD COLUMN IF NOT EXISTS `qa_reply_intervals_secs` Array(Int64) AFTER `qa_cnt`,
ADD COLUMN IF NOT EXISTS `m_qa_cnt` Int64 AFTER `qa_reply_intervals_secs`,
ADD COLUMN IF NOT EXISTS `m_qa_reply_intervals_secs` Array(Int64) AFTER `m_qa_cnt`;


-- ft_dwd.order_detail_all
ALTER TABLE ft_dwd.order_detail_local ON CLUSTER cluster_3s_2r
RENAME COLUMN original_sratus TO original_status

ALTER TABLE ft_dwd.order_detail_all ON CLUSTER cluster_3s_2r
RENAME COLUMN original_sratus TO original_status
