-- 由于需求变更, 表结构可以暂时不更新

-- 新接入xqc.shop表
-- PS: 明察质检企业开通店铺表
{
    '_id': ObjectId('6102952f181d4300011d13ce'),
    'create_time': datetime.datetime(2021, 7, 29, 11, 46, 55, 350000),
    'update_time': datetime.datetime(2021, 7, 29, 11, 46, 55, 350000),
    'company_id': ObjectId('61028f0c181d4300011d1308'),
    'shop_id': ObjectId('60efdc660142ab0014fcae7d'),
    'platform': 'tb',
    'seller_nick': '害怕一瞬间',
    'plat_shop_name': '害怕一瞬间',
    'plat_shop_id': ''
}

-- xqc.qc_norm
{
    '_id': ObjectId('5f918b73d8a5eb0001fc679d'),
    'create_time': datetime.datetime(2020, 10, 22, 13, 38, 59, 803000),
    'update_time': datetime.datetime(2021, 11, 15, 1, 20, 54, 225000),
    'company_id': ObjectId('5f747ba42c90fd0001254404'),
    'platform': 'tb',
    'name': '售前标准-天猫渠道',
    'description': '',
    'status': 1,
    'emotion_detect_mode': 1,
    'check_message': True
}
-- tb,mini修改表结构
ALTER TABLE tmp.xinghuan_qc_norm_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS platform String AFTER company_id,
ADD COLUMN IF NOT EXISTS check_message String AFTER company_id

ALTER TABLE tmp.xinghuan_qc_norm_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS platform String AFTER company_id,
ADD COLUMN IF NOT EXISTS check_message String AFTER company_id

ALTER TABLE ods.xinghuan_qc_norm_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS platform String AFTER company_id,
ADD COLUMN IF NOT EXISTS check_message String AFTER company_id

ALTER TABLE ods.xinghuan_qc_norm_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS platform String AFTER company_id,
ADD COLUMN IF NOT EXISTS check_message String AFTER company_id

-- ks,jd修改表结构
ALTER TABLE tmp.xinghuan_qc_norm
ADD COLUMN IF NOT EXISTS platform String AFTER company_id,
ADD COLUMN IF NOT EXISTS check_message String AFTER company_id

ALTER TABLE ods.xinghuan_qc_norm_all
ADD COLUMN IF NOT EXISTS platform String AFTER company_id,
ADD COLUMN IF NOT EXISTS check_message String AFTER company_id

