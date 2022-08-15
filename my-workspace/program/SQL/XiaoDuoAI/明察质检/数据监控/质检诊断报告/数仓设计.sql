-- 质检项分组统计

-- 统计维度: 天/平台/主账号/子账号/质检标准/质检项分组(一级和二级)
-- 度量: 扣分会话数, 加分会话数

1.xqc_dim.qc_norm_group_full_all
增加字段 super_group_ids, 且level=length(super_group_ids), 用于存储根节点到叶子节点的完整链路, 适当冗余, 支持扩展

-- xqc_dim.qc_norm_group_full_all
ALTER TABLE xqc_dim.qc_norm_group_full_local ON CLUSTER cluster_3s_2r
ADD COLUMN super_group_ids String AFTER `parent_id`

ALTER TABLE xqc_dim.qc_norm_group_full_all ON CLUSTER cluster_3s_2r
ADD COLUMN super_group_ids String AFTER `parent_id`

2. xqc_dim.qc_rule_all 通过 qc_norm_id 和 qc_group_id 关联 xqc_dim.qc_norm_group_full_all, 以获取质检项一级和二级分组id

3. dwd.xdqc_dialog 查询当天/平台/主账号/子账号/质检项ID/质检项分数, 过滤质检项为空的记录, 过滤加分和扣分为0的记录
展开质检项ID和质检项分值, 关联表2, 获取一级和二级分组, 注意数据会膨胀

4. 统计表3中, 各个一级分组下对应质检项为加分会话数、扣分会话数, level=1

5. 统计表3中, 各个二级分组下对应质检项为加分会话数、扣分会话数, level=2

CREATE DATABASE IF NOT EXISTS xqc_dws ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE xqc_dws.tag_group_stat_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dws.tag_group_stat_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `seller_nick` String,
    `snick` String,
    `qc_norm_id` String,
    `tag_group_id` String,
    `tag_group_level` Int64,
    `add_score_dialog_cnt` Int64,
    `subtract_score_dialog_cnt` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_dws.tag_group_stat_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dws.tag_group_stat_all ON CLUSTER cluster_3s_2r
AS xqc_dws.tag_group_stat_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dws', 'tag_group_stat_local', rand())


-- 统计维度: 天/平台/主账号/子账号/质检标准
-- 度量: 扣分会话数, 加分会话数, 质检会话数
1. 关联 ods.xinghuan_qc_norm_relate_all 和 ods.xinghuan_employee_snick_all 表, 获取每个质检表准下的子账号, 以及对应的质检标准id, 
筛选质检标准id不为空的记录

2. dwd.xdqc_dialog 查询当天/平台/主账号/子账号, 并通过子账号关联 表1, 获取对应质检标准id

3. 统计当天/平台/主账号/子账号下的质检标准, 扣分会话数, 加分会话数, 质检会话数

CREATE DATABASE IF NOT EXISTS xqc_dws ON CLUSTER cluster_3s_2r
ENGINE=Ordinary


-- 统计维度: 天/平台/主账号/子账号/质检标准
-- 度量: 扣分会话数, 加分会话数, 会话总量(已存在)
xqc_dws.snick_stat_all
    扣分会话数(add_score_dialog_cnt), 
    加分会话数(subtract_score_dialog_cnt), 
    质检标准id(qc_norm_id)

