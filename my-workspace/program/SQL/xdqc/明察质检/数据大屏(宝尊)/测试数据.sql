-- 方太测试数据
-- 告警表测试数据
TRUNCATE TABLE xqc_ods.alert_local ON CLUSTER cluster_3s_2r
INSERT INTO buffer.xqc_ods_alert_buffer
SELECT
    id,
    platform,
    type AS level,
    reason AS warning_type,
    dialog_id,
    'test' AS message_id,
    create_time AS time,
    day,
    done as is_finished,
    if(done='True',toString(now()),'') as finish_time,
    shop_id,
    seller_nick,
    snick,
    cnick,
    'test' AS employee_name,
    'test' AS superior_name,
    if(done='True',now(),parseDateTimeBestEffort(create_time)) as update_time
FROM xqc_ods.event_alert_all
WHERE day BETWEEN 20210811 AND 20210923

-- 会话表测试数据
TRUNCATE TABLE xqc_ods.dialog_local ON CLUSTER cluster_3s_2r
INSERT INTO buffer.xqc_ods_dialog_buffer
SELECT
    dialog_id AS id,
    platform,
    shop_id,
    seller_nick,
    snick,
    cnick,
    '' AS employee_name,
    '' AS superior_name,
    update_time AS time,
    toHour(parseDateTimeBestEffort(update_time)) AS hour,
    day
FROM xqc_ods.event_alert_all
WHERE day BETWEEN 20210811 AND 20210923


-- 组织架构表测试数据
ALTER TABLE xqc_dim.group_local ON CLUSTER cluster_3s_2r 
DELETE WHERE company_id='5f747ba42c90fd0001254404'

INSERT INTO xqc_dim.group_all
VALUES 
('5f747ba42c90fd0001254404','方太','','','5f747ba42c90fd0001254401','一级部门1','False','',[],1),
('5f747ba42c90fd0001254404','方太','','','5f747ba42c90fd0001254402','一级部门2','False','',[],1),
('5f747ba42c90fd0001254404','方太','','','5f747ba42c90fd0001254403','一级部门3','False','',[],1),
('5f747ba42c90fd0001254404','方太','','','5f747ba42c90fd0001254404','二级部门1','False','',['5f747ba42c90fd0001254401'],2),
('5f747ba42c90fd0001254404','方太','','','5f747ba42c90fd0001254405','二级部门2','False','',['5f747ba42c90fd0001254403'],2),
('5f747ba42c90fd0001254404','方太','','','5edfa47c8f591c00163ef7d6','方太京东旗舰店','True','jd',['5f747ba42c90fd0001254401','5f747ba42c90fd0001254404'],3),
('5f747ba42c90fd0001254404','方太','','','5e9d390d68283c002457b52f','方太京东自营旗舰店','True','jd',['5f747ba42c90fd0001254402'],2)
('5f747ba42c90fd0001254404','方太','','','5cac112e98ef4100118a9c9f','方太官方旗舰店','True','tb',['5f747ba42c90fd0001254403','5f747ba42c90fd0001254405'],3)

-- 测试参数
company_id=5f747ba42c90fd0001254404
-- 权限隔离测试参数
shop_id_list=5cac112e98ef4100118a9c9f
snick_list=方太官方旗舰店:柚子


-- 宝尊测试数据
-- 测试参数
company_id=6131e6554524490001fc6825
-- 权限隔离测试参数
shop_id_list=61318f916ebd17000f941d0b
snick_list=Origins悦木之源京东自营官方旗舰店:柚子
-- 权限隔离测试参数(改)
shop_id_list=6139c118e16787000fb8a1cf
snick_list=维多利亚的秘密美妆官方旗舰店:方太电器售后琴琴
-- 告警表测试数据
-- TRUNCATE TABLE xqc_ods.alert_local ON CLUSTER cluster_3s_2r
INSERT INTO buffer.xqc_ods_alert_buffer
SELECT
    toString(generateUUIDv4()) AS id,
    'jd' AS platform,
    type AS level,
    reason AS warning_type,
    dialog_id,
    'test' AS message_id,
    create_time AS time,
    day,
    done as is_finished,
    if(done='True',toString(now()),'') as finish_time,
    CASE
        WHEN shop_id='5edfa47c8f591c00163ef7d6' THEN '5cd268e42bf9a8000f9301d7'
        WHEN shop_id='5e9d390d68283c002457b52f' THEN '6139c118e16787000fb8a1cf'
        WHEN shop_id='5cac112e98ef4100118a9c9f' THEN '61318f916ebd17000f941d0b'
    END,
    CASE
        WHEN shop_id='5edfa47c8f591c00163ef7d6' THEN 'PUMA官方旗舰店'
        WHEN shop_id='5e9d390d68283c002457b52f' THEN '维多利亚的秘密美妆官方旗舰店'
        WHEN shop_id='5cac112e98ef4100118a9c9f' THEN 'Origins悦木之源京东自营官方旗舰店'
    END,
    CASE
        WHEN shop_id='5edfa47c8f591c00163ef7d6' THEN replaceRegexpOne(snick,'.*:','PUMA官方旗舰店:')
        WHEN shop_id='5e9d390d68283c002457b52f' THEN replaceRegexpOne(snick,'.*:','维多利亚的秘密美妆官方旗舰店:')
        WHEN shop_id='5cac112e98ef4100118a9c9f' THEN replaceRegexpOne(snick,'.*:','Origins悦木之源京东自营官方旗舰店:')
    END,
    cnick,
    'test' AS employee_name,
    'test' AS superior_name,
    if(done='True',now(),parseDateTimeBestEffort(create_time)) as update_time
FROM xqc_ods.event_alert_all
WHERE day BETWEEN 20210811 AND 20210925
AND shop_id IN ('5edfa47c8f591c00163ef7d6','5e9d390d68283c002457b52f','5cac112e98ef4100118a9c9f')


-- 会话表测试数据
-- TRUNCATE TABLE xqc_ods.dialog_local ON CLUSTER cluster_3s_2r
INSERT INTO buffer.xqc_ods_dialog_buffer
SELECT
    dialog_id AS id,
    platform,
    CASE
        WHEN shop_id='5edfa47c8f591c00163ef7d6' THEN '5cd268e42bf9a8000f9301d7'
        WHEN shop_id='5e9d390d68283c002457b52f' THEN '6139c118e16787000fb8a1cf'
        WHEN shop_id='5cac112e98ef4100118a9c9f' THEN '61318f916ebd17000f941d0b'
    END,
    CASE
        WHEN shop_id='5edfa47c8f591c00163ef7d6' THEN 'PUMA官方旗舰店'
        WHEN shop_id='5e9d390d68283c002457b52f' THEN '维多利亚的秘密美妆官方旗舰店'
        WHEN shop_id='5cac112e98ef4100118a9c9f' THEN 'Origins悦木之源京东自营官方旗舰店'
    END,
    CASE
        WHEN shop_id='5edfa47c8f591c00163ef7d6' THEN replaceRegexpOne(snick,'.*:','PUMA官方旗舰店:')
        WHEN shop_id='5e9d390d68283c002457b52f' THEN replaceRegexpOne(snick,'.*:','维多利亚的秘密美妆官方旗舰店:')
        WHEN shop_id='5cac112e98ef4100118a9c9f' THEN replaceRegexpOne(snick,'.*:','Origins悦木之源京东自营官方旗舰店:')
    END,
    cnick,
    'test' AS employee_name,
    'test' AS superior_name,
    update_time AS time,
    toHour(parseDateTimeBestEffort(update_time)) AS hour,
    day
FROM xqc_ods.event_alert_all
WHERE day BETWEEN 20210811 AND 20210925
AND shop_id IN ('5edfa47c8f591c00163ef7d6','5e9d390d68283c002457b52f','5cac112e98ef4100118a9c9f')


-- 组织架构表测试数据
INSERT INTO xqc_dim.group_all
VALUES 
('6131e6554524490001fc6825','宝尊',toString(now()),toString(now()),'5cd268e42bf9a8000f9301d0','一级部门1','False','',[],1),
('6131e6554524490001fc6825','宝尊',toString(now()),toString(now()),'5cd268e42bf9a8000f9301d1','一级部门2','False','',[],1),
('6131e6554524490001fc6825','宝尊',toString(now()),toString(now()),'5cd268e42bf9a8000f9301d2','一级部门3','False','',[],1),
('6131e6554524490001fc6825','宝尊',toString(now()),toString(now()),'5cd268e42bf9a8000f9301d3','二级部门1','False','',['5cd268e42bf9a8000f9301d0'],2),
('6131e6554524490001fc6825','宝尊',toString(now()),toString(now()),'5cd268e42bf9a8000f9301d4','二级部门2','False','',['5cd268e42bf9a8000f9301d1'],2),
('6131e6554524490001fc6825','宝尊',toString(now()),toString(now()),'5cd268e42bf9a8000f9301d5','二级部门3','False','',['5cd268e42bf9a8000f9301d2'],2),
('6131e6554524490001fc6825','宝尊',toString(now()),toString(now()),'5cd268e42bf9a8000f9301d7','PUMA官方旗舰店','True','jd',['5cd268e42bf9a8000f9301d0','5cd268e42bf9a8000f9301d3'],3),
('6131e6554524490001fc6825','宝尊',toString(now()),toString(now()),'6139c118e16787000fb8a1cf','维多利亚的秘密美妆官方旗舰店','True','jd',['5cd268e42bf9a8000f9301d1','5cd268e42bf9a8000f9301d4'],3)
('6131e6554524490001fc6825','宝尊',toString(now()),toString(now()),'61318f916ebd17000f941d0b','Origins悦木之源京东自营官方旗舰店','True','tb',['5cd268e42bf9a8000f9301d2','5cd268e42bf9a8000f9301d5'],3)