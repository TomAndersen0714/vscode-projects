-- dim.session_msg_detail_all
-- ALTER TABLE dim.session_msg_detail_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `send_msg_from`;
ALTER TABLE dim.session_msg_detail_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `send_msg_from` Int64 AFTER `is_first_msg_within_session`;

-- ALTER TABLE dim.session_msg_detail_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `send_msg_from`;
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
