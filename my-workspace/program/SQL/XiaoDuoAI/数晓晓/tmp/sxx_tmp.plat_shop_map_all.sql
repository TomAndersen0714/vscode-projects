CREATE DATABASE sxx_tmp ON CLUSTER cluster_3s_2r

-- DROP TABLE sxx_tmp.plat_shop_map_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_tmp.plat_shop_map_local ON CLUSTER cluster_3s_2r
(
    `platform` String,
    `platform_cn` String,
    `custom_shop_name` String,
    `shop_name` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (platform)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE sxx_tmp.plat_shop_map_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_tmp.plat_shop_map_all ON CLUSTER cluster_3s_2r
AS sxx_tmp.plat_shop_map_local
ENGINE = Distributed('cluster_3s_2r', 'sxx_tmp', 'plat_shop_map_local', rand())

-- DROP TABLE buffer.sxx_tmp_plat_shop_map_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.sxx_tmp_plat_shop_map_buffer ON CLUSTER cluster_3s_2r
AS sxx_tmp.plat_shop_map_all
ENGINE = Buffer('sxx_tmp', 'plat_shop_map_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)

-- TRUNCATE TABLE sxx_tmp.plat_shop_map_local ON CLUSTER cluster_3s_2r
INSERT INTO TABLE sxx_tmp.plat_shop_map_all(platform, platform_cn, custom_shop_name, shop_name)
VALUES
('jd', '京东', '(京东) 红小厨京东自营旗舰店', '红小厨京东自营旗舰店')
('jd', '京东', '(京东)星农官方旗舰店', '红小厨京东自营旗舰店')
('dy', '抖音', '(抖音)红小厨食品旗舰店', '红小厨食品旗舰店')
('gmpt', '国美平台（真快乐）', '(国美)红小厨旗舰店', '红小厨旗舰店')
('jd', '京东', '(京东)红小厨旗舰店', '红小厨旗舰店')
('ks', '快手', '(快手)红小厨旗舰店', '红小厨旗舰店')
('ks', '快手', '（快手）红小厨食品旗舰店', '')
('ks', '快手', '(快手)红小厨食品旗舰店', '')
('other', '其他', '(魔筷星选)红小厨旗舰店', '其它店铺')
('pdd', '拼多多', '(拼多多)红小厨旗舰店', '红小厨旗舰店')
('sn', '苏宁', '(苏宁)红小厨旗舰店', '红小厨旗舰店')
('tb', '淘宝', '(天猫)红小厨旗舰店', '红小厨旗舰店')
('tb', '淘宝', '(天猫)星农联合旗舰店', '星农联合旗舰店')
('yz', '有赞', '(有赞)红小厨商城', '红小厨旗舰店')
('mt', '美团', '美团-团好货', '团好货')
('sn', '苏宁', '苏宁自营', '苏宁红小厨自营')
('xm', '小米', '小米有品自营店铺', '红小厨旗舰店')
('jd', '京东', '星农联合京东自营官方旗舰店', '星农联合京东自营官方旗舰店')
('klhg', '考拉海购', '(网易考拉)红小厨官方旗舰店', '红小厨官方旗舰店')
('jd', '京东', '(京东) 红小厨京东自营旗舰店', '红小厨京东自营旗舰店')
('jd', '京东', '(京东)星农官方旗舰店', '星农联合官方旗舰店')
('other', '其他', '谢云现货推送', '其它店铺')
('mgj', '蘑菇街', '蘑菇街', '红小厨旗舰店')
('jd', '京东', '(京东)红小厨生鲜旗舰店', '红小厨生鲜旗舰店')
('akc', '爱库存', '红小厨-小店', '红小厨旗舰店')
('tt', '淘特', 'HXC生鲜企业店', '红小厨旗舰店')
('sqtg', '社区团购', '盒马集市', '盒马集市')
('sqtg', '社区团购', '美团优选', '美团优选')
('dy', '抖音', '(抖音)红小厨官方旗舰店', '红小厨官方旗舰店')
('klhg', '考拉海购', '考拉自营', '考拉自营')
('ks', '快手', '(快手)红小厨官方旗舰店', '红小厨官方旗舰店')
('dy', '抖音', '(抖音)红小厨生鲜旗舰店', '红小厨生鲜旗舰店')
('jd', '京东', '(京东)红小厨京东自营旗舰店', '红小厨京东自营旗舰店')
('tb', '淘宝', '(渠道)天猫供销平台', '苏宁易购官方旗舰店')
('mt', '美团', '团好货自营', '团好货')
('xhs', '小红书', '(小红书)红小厨旗舰店', '红小厨旗舰店')
('ks', '快手', '(快手)红小厨食品专营店', '红小厨食品专营店')
('tx', '腾讯', '腾讯惠聚', '腾讯惠聚')
('jl', '鲸灵', 'H品牌特卖', '鲸灵')
('pdd', '拼多多', '(拼多多)红小厨水产旗舰店', '红小厨水产旗舰店')
('jd', '京东', '（京东）阳澄联合京东自营官方旗舰店', '阳澄联合京东自营官方旗舰店')
('kszy','考拉自营', '考拉自营HXC', '')