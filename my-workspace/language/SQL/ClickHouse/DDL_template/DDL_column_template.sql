-- dim.session_msg_detail_all
-- ALTER TABLE dim.session_msg_detail_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `send_msg_from` NO DELAY
ALTER TABLE dim.session_msg_detail_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `send_msg_from` Int64 AFTER `is_first_msg_within_session`;

-- ALTER TABLE dim.session_msg_detail_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `send_msg_from` NO DELAY
ALTER TABLE dim.session_msg_detail_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `send_msg_from` Int64 AFTER `is_first_msg_within_session`;

-- dim.session_detail_all
ALTER TABLE dim.session_detail_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `m_session_send_cnt` Int64 AFTER `session_send_cnt`,
ADD COLUMN IF NOT EXISTS `qa_cnt` Int64 AFTER `m_session_send_cnt`,
ADD COLUMN IF NOT EXISTS `qa_reply_intervals_secs` Array(Int64) AFTER `qa_cnt`,
ADD COLUMN IF NOT EXISTS `m_qa_cnt` Int64 AFTER `qa_reply_intervals_secs`,
ADD COLUMN IF NOT EXISTS `m_qa_reply_intervals_secs` Array(Int64) AFTER `m_qa_cnt`;

ALTER TABLE dim.session_detail_all ON CLUSTER cluster_3s_2r
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


-- dim.voc_question_b_detail_all
ALTER TABLE dim.voc_question_b_detail_local ON CLUSTER cluster_3s_2r
RENAME COLUMN sid TO question_b_id

ALTER TABLE dim.voc_question_b_detail_all ON CLUSTER cluster_3s_2r
RENAME COLUMN sid TO question_b_id
