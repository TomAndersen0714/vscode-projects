-- 老淘宝: 添加platform字段, 并将对应的值设置为tb
ALTER TABLE tmp.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r ADD COLUMN platform String FIRST
ALTER TABLE tmp.xinghuan_dialog_tag_score_all ON CLUSTER cluster_3s_2r ADD COLUMN platform String FIRST
ALTER TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r ADD COLUMN platform String FIRST
ALTER TABLE ods.xinghuan_dialog_tag_score_all ON CLUSTER cluster_3s_2r ADD COLUMN platform String FIRST

ALTER TABLE tmp.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r UPDATE platform='tb'
ALTER TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r UPDATE platform='tb'