-- 老淘宝: 统计表添加platform字段, 并将对应的值设置为tb

-- ods.xinghuan_dialog_tag_score_all
ALTER TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r ADD COLUMN platform String FIRST
ALTER TABLE ods.xinghuan_dialog_tag_score_all ON CLUSTER cluster_3s_2r ADD COLUMN platform String FIRST

-- PS: FIRST指令在v20.6及之后才支持:https://clickhouse.com/docs/en/whats-new/changelog/2020/#new-feature_6
-- 对于之前的ClickHouse, 可以尝试通过 MODIFY COLUMN 修改首列的位置
ALTER TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r MODIFY COLUMN platform AFTER seller_nick
ALTER TABLE ods.xinghuan_dialog_tag_score_all ON CLUSTER cluster_3s_2r MODIFY COLUMN platform AFTER seller_nick
-- 手动设置默认值
ALTER TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r UPDATE platform='tb' WHERE 1=1

-- ods.qc_statistical_employee_all
ALTER TABLE ods.qc_statistical_employee_local ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER company_id
ALTER TABLE ods.qc_statistical_employee_all ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER company_id
-- 手动设置默认值
ALTER TABLE ods.qc_statistical_employee_local ON CLUSTER cluster_3s_2r UPDATE platform='tb' WHERE 1=1

-- ods.qc_statistical_department_all
ALTER TABLE ods.qc_statistical_department_local ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER company_id
ALTER TABLE ods.qc_statistical_department_all ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER company_id
-- 手动设置默认值
ALTER TABLE ods.qc_statistical_department_local ON CLUSTER cluster_3s_2r UPDATE platform='tb' WHERE 1=1