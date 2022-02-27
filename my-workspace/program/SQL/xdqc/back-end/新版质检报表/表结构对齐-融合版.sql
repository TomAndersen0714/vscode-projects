-- 注意: 修改表结构和修改任务必须同步进行

-- 融合版表结构表更, 与老淘宝对齐
/*
ods.qc_statistical_employee_local
ods.qc_statistical_employee_all
*/
ALTER TABLE ods.qc_statistical_employee_local ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER company_id
ALTER TABLE ods.qc_statistical_employee_all ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER company_id
ALTER TABLE ods.qc_statistical_employee_local ON CLUSTER cluster_3s_2r UPDATE platform='tb' WHERE 1=1

/* 
ods.qc_statistical_department_local
ods.qc_statistical_department_all
*/
ALTER TABLE ods.qc_statistical_department_local ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER company_id
ALTER TABLE ods.qc_statistical_department_all ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER company_id
ALTER TABLE ods.qc_statistical_department_local ON CLUSTER cluster_3s_2r UPDATE platform='tb' WHERE 1=1

/*
ods.xinghuan_dialog_tag_score_local
ods.xinghuan_dialog_tag_score_all
*/

ALTER TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER `seller_nick`
ALTER TABLE ods.xinghuan_dialog_tag_score_all ON CLUSTER cluster_3s_2r ADD COLUMN platform String AFTER `seller_nick`
ALTER TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r UPDATE platform='tb' WHERE 1=1


/* 
tmp.xinghuan_customize_rule_local
tmp.xinghuan_customize_rule_all
ods.xinghuan_customize_rule_local
ods.xinghuan_customize_rule_all
 */

ALTER TABLE tmp.xinghuan_customize_rule_local ON CLUSTER cluster_3s_2r
ADD COLUMN company_id String AFTER `update_time`,
ADD COLUMN qc_norm_id String AFTER `platform`

ALTER TABLE tmp.xinghuan_customize_rule_all ON CLUSTER cluster_3s_2r
ADD COLUMN company_id String AFTER `update_time`,
ADD COLUMN qc_norm_id String AFTER `platform`

ALTER TABLE ods.xinghuan_customize_rule_local ON CLUSTER cluster_3s_2r
ADD COLUMN company_id String AFTER `update_time`,
ADD COLUMN qc_norm_id String AFTER `platform`

ALTER TABLE ods.xinghuan_customize_rule_all ON CLUSTER cluster_3s_2r
ADD COLUMN company_id String AFTER `update_time`,
ADD COLUMN qc_norm_id String AFTER `platform`

/*
buffer.xdqc_dialog_update_buffer
ods.xdqc_dialog_update_all
ods.xdqc_dialog_update_local
buffer.xdqc_dialog_buffer
ods.xdqc_dialog_all
ods.xdqc_dialog_local
dwd.xdqc_dialog_all
dwd.xdqc_dialog_local
*/

-- ods.xdqc_dialog_local
ALTER TABLE ods.xdqc_dialog_local ON CLUSTER cluster_3s_2r
ADD COLUMN tag_score_stats.count Array(UInt32) AFTER `tag_score_stats.score`,
ADD COLUMN tag_score_stats.md Array(UInt8) AFTER `tag_score_stats.count`,
ADD COLUMN tag_score_stats.mm Array(UInt8) AFTER `tag_score_stats.md`,
ADD COLUMN tag_score_add_stats.count Array(UInt32) AFTER `tag_score_add_stats.score`,
ADD COLUMN tag_score_add_stats.md Array(UInt8) AFTER `tag_score_add_stats.count`,
ADD COLUMN tag_score_add_stats.mm Array(UInt8) AFTER `tag_score_add_stats.md`
-- ods.xdqc_dialog_all
ALTER TABLE ods.xdqc_dialog_all ON CLUSTER cluster_3s_2r
ADD COLUMN tag_score_stats.count Array(UInt32) AFTER `tag_score_stats.score`,
ADD COLUMN tag_score_stats.md Array(UInt8) AFTER `tag_score_stats.count`,
ADD COLUMN tag_score_stats.mm Array(UInt8) AFTER `tag_score_stats.md`,
ADD COLUMN tag_score_add_stats.count Array(UInt32) AFTER `tag_score_add_stats.score`,
ADD COLUMN tag_score_add_stats.md Array(UInt8) AFTER `tag_score_add_stats.count`,
ADD COLUMN tag_score_add_stats.mm Array(UInt8) AFTER `tag_score_add_stats.md`
-- buffer.xdqc_dialog_buffer
DROP TABLE buffer.xdqc_dialog_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xdqc_dialog_buffer ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_all
ENGINE = Buffer('ods', 'xdqc_dialog_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

-- ods.xdqc_dialog_update_local
ALTER TABLE ods.xdqc_dialog_update_local ON CLUSTER cluster_3s_2r
ADD COLUMN tag_score_stats.count Array(UInt32) AFTER `tag_score_stats.score`,
ADD COLUMN tag_score_stats.md Array(UInt8) AFTER `tag_score_stats.count`,
ADD COLUMN tag_score_stats.mm Array(UInt8) AFTER `tag_score_stats.md`,
ADD COLUMN tag_score_add_stats.count Array(UInt32) AFTER `tag_score_add_stats.score`,
ADD COLUMN tag_score_add_stats.md Array(UInt8) AFTER `tag_score_add_stats.count`,
ADD COLUMN tag_score_add_stats.mm Array(UInt8) AFTER `tag_score_add_stats.md`
-- ods.xdqc_dialog_update_all
ALTER TABLE ods.xdqc_dialog_update_all ON CLUSTER cluster_3s_2r
ADD COLUMN tag_score_stats.count Array(UInt32) AFTER `tag_score_stats.score`,
ADD COLUMN tag_score_stats.md Array(UInt8) AFTER `tag_score_stats.count`,
ADD COLUMN tag_score_stats.mm Array(UInt8) AFTER `tag_score_stats.md`,
ADD COLUMN tag_score_add_stats.count Array(UInt32) AFTER `tag_score_add_stats.score`,
ADD COLUMN tag_score_add_stats.md Array(UInt8) AFTER `tag_score_add_stats.count`,
ADD COLUMN tag_score_add_stats.mm Array(UInt8) AFTER `tag_score_add_stats.md`
-- buffer.xdqc_dialog_update_buffer
DROP TABLE buffer.xdqc_dialog_update_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xdqc_dialog_update_buffer ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_update_all
ENGINE = Buffer('ods', 'xdqc_dialog_update_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

-- dwd.xdqc_dialog_local
ALTER TABLE dwd.xdqc_dialog_local ON CLUSTER cluster_3s_2r
ADD COLUMN tag_score_stats_count Array(UInt32) AFTER `tag_score_stats_score`,
ADD COLUMN tag_score_stats_md Array(UInt8) AFTER `tag_score_stats_count`,
ADD COLUMN tag_score_stats_mm Array(UInt8) AFTER `tag_score_stats_md`,
ADD COLUMN tag_score_add_stats_count Array(UInt32) AFTER `tag_score_add_stats_score`,
ADD COLUMN tag_score_add_stats_md Array(UInt8) AFTER `tag_score_add_stats_count`,
ADD COLUMN tag_score_add_stats_mm Array(UInt8) AFTER `tag_score_add_stats_md`
-- dwd.xdqc_dialog_all
ALTER TABLE dwd.xdqc_dialog_all ON CLUSTER cluster_3s_2r
ADD COLUMN tag_score_stats_count Array(UInt32) AFTER `tag_score_stats_score`,
ADD COLUMN tag_score_stats_md Array(UInt8) AFTER `tag_score_stats_count`,
ADD COLUMN tag_score_stats_mm Array(UInt8) AFTER `tag_score_stats_md`,
ADD COLUMN tag_score_add_stats_count Array(UInt32) AFTER `tag_score_add_stats_score`,
ADD COLUMN tag_score_add_stats_md Array(UInt8) AFTER `tag_score_add_stats_count`,
ADD COLUMN tag_score_add_stats_mm Array(UInt8) AFTER `tag_score_add_stats_md`