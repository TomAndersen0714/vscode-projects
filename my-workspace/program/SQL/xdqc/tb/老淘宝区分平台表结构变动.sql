-- 老淘宝: 统计表添加platform字段, 并将对应的值设置为tb

-- ods.xinghuan_dialog_tag_score_all
ALTER TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r ADD COLUMN platform String FIRST
ALTER TABLE ods.xinghuan_dialog_tag_score_all ON CLUSTER cluster_3s_2r ADD COLUMN platform String FIRST

-- PS: FIRST指令在v20.6及之后才支持:https://clickhouse.com/docs/en/whats-new/changelog/2020/#new-feature_6
-- 对于之前的ClickHouse, 无法控制列的顺序, 因此对于这种情况, 建议如果数据量不大的话, 备份+重建表格
-- https://github.com/ClickHouse/ClickHouse/blob/v20.4.2.9-stable/docs/en/sql_reference/statements/alter.md#alter_add-column

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

-- PS: 修改表结构和修改任务必须同步进行