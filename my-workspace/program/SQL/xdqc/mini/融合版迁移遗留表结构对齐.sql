融合版:
-- seller_nick字段改名为qc_norm_id:
ods.xdqc_abnormal_check_setting_local
ods.xdqc_abnormal_check_setting_all

ods.xdqc_excellent_check_setting_local
ods.xdqc_excellent_check_setting_all

ods.xdqc_emotion_check_item_local
ods.xdqc_emotion_check_item_all

ods.xdqc_tag_sub_category_local
ods.xdqc_tag_sub_category_all

-- 在platform字段之后增加company_id:
tmp.xdqc_abnormal_check_setting_local
tmp.xdqc_abnormal_check_setting_all
ods.xdqc_abnormal_check_setting_local
ods.xdqc_abnormal_check_setting_all

tmp.xdqc_excellent_check_setting_local
tmp.xdqc_excellent_check_setting_all
ods.xdqc_excellent_check_setting_local
ods.xdqc_excellent_check_setting_all

tmp.xdqc_emotion_check_item_local
tmp.xdqc_emotion_check_item_all
ods.xdqc_emotion_check_item_local
ods.xdqc_emotion_check_item_all

tmp.xdqc_tag_local
tmp.xdqc_tag_all
ods.xdqc_tag_local
ods.xdqc_tag_all

tmp.xinghuan_qc_norm_relate_local
tmp.xinghuan_qc_norm_relate_all
ods.xinghuan_qc_norm_relate_local
ods.xinghuan_qc_norm_relate_all


-- 京东
-- seller_nick字段改名为qc_norm_id:
ALTER TABLE tmp.xdqc_abnormal_check_setting ADD COLUMN qc_norm_id String AFTER seller_nick
ALTER TABLE ods.xdqc_abnormal_check_setting_all ADD COLUMN qc_norm_id String AFTER seller_nick

ALTER TABLE tmp.xdqc_excellent_check_setting ADD COLUMN qc_norm_id String AFTER seller_nick
ALTER TABLE ods.xdqc_excellent_check_setting_all ADD COLUMN qc_norm_id String AFTER seller_nick

ALTER TABLE tmp.xdqc_emotion_check_item ADD COLUMN qc_norm_id String AFTER seller_nick
ALTER TABLE ods.xdqc_emotion_check_item_all ADD COLUMN qc_norm_id String AFTER seller_nick

ALTER TABLE tmp.xdqc_tag_sub_category ADD COLUMN qc_norm_id String AFTER seller_nick
ALTER TABLE ods.xdqc_tag_sub_category_all ADD COLUMN qc_norm_id String AFTER seller_nick


-- 在platform字段之后增加company_id:
ALTER TABLE tmp.xdqc_abnormal_check_setting ADD COLUMN company_id String AFTER platform
ALTER TABLE ods.xdqc_abnormal_check_setting_all ADD COLUMN company_id String AFTER platform

ALTER TABLE tmp.xdqc_excellent_check_setting ADD COLUMN company_id String AFTER platform
ALTER TABLE ods.xdqc_excellent_check_setting_all ADD COLUMN company_id String AFTER platform

ALTER TABLE tmp.xdqc_emotion_check_item ADD COLUMN company_id String AFTER platform
ALTER TABLE ods.xdqc_emotion_check_item_all ADD COLUMN company_id String AFTER platform

ALTER TABLE tmp.xdqc_tag ADD COLUMN company_id String AFTER platform
ALTER TABLE ods.xdqc_tag_all ADD COLUMN company_id String AFTER platform

ALTER TABLE tmp.xinghuan_qc_norm_relate ADD COLUMN company_id String AFTER platform
ALTER TABLE ods.xinghuan_qc_norm_relate_all ADD COLUMN company_id String AFTER platform



