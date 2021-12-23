-- 老淘宝: 统计表添加platform字段, 并将对应的值设置为tb

-- ods.xinghuan_dialog_tag_score_all
ALTER TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r ADD COLUMN platform String FIRST
ALTER TABLE ods.xinghuan_dialog_tag_score_all ON CLUSTER cluster_3s_2r ADD COLUMN platform String FIRST

ALTER TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r UPDATE platform='tb' WHERE 1=1

-- ods.qc_statistical_employee_all
ALTER TABLE ods.qc_statistical_employee_local ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER company_id
ALTER TABLE ods.qc_statistical_employee_all ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER company_id

ALTER TABLE ods.qc_statistical_employee_local ON CLUSTER cluster_3s_2r UPDATE platform='tb' WHERE 1=1

-- ods.qc_statistical_department_all
ALTER TABLE ods.qc_statistical_department_local ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER company_id
ALTER TABLE ods.qc_statistical_department_all ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER company_id

ALTER TABLE ods.qc_statistical_department_local ON CLUSTER cluster_3s_2r UPDATE platform='tb' WHERE 1=1