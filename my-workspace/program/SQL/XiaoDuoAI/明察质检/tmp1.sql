ALTER TABLE sxx_dwd.voc_qc_compensate_local ON CLUSTER cluster_3s_2r
DROP COLUMN `qc_cnt`

ALTER TABLE sxx_dwd.voc_qc_compensate_all ON CLUSTER cluster_3s_2r
DROP COLUMN `qc_cnt`


ALTER TABLE sxx_dwd.voc_qc_compensate_local ON CLUSTER cluster_3s_2r
ADD COLUMN `qc_label_cnt` Int64 AFTER `qc_label_id`

ALTER TABLE sxx_dwd.voc_qc_compensate_all ON CLUSTER cluster_3s_2r
ADD COLUMN `qc_label_cnt` Int64 AFTER `qc_label_id`