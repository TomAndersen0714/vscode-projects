-- 质检项分组统计

-- 统计维度: 天/平台/主账号/子账号/质检标准/质检项分组(一级和二级)
-- 度量: 扣分会话数, 加分会话数
xqc_dim.qc_norm_group_full_all
    super_group_ids, 用于存储根节点到叶子节点的完整链路, 适当冗余, 支持扩展
    level=length(super_group_ids)

xqc_dws.tag_group_stat_all


1. xqc_dim.qc_rule_all 通过 qc_norm_id 和 qc_group_id 关联 xqc_dim.qc_norm_group_full_all, 以获取质检项一级和二级分组id

2. dwd.xdqc_dialog 查询当天/平台/主账号/子账号/质检项ID/质检项分数, 过滤质检项为空的记录, 过滤加分和扣分为0的记录
展开质检项ID和质检项分值, 关联表2, 获取一级和二级分组, 注意数据会膨胀

3. 统计表3中, 各个一级分组下对应质检项为加分会话数、扣分会话数, level=1

4. 统计表3中, 各个二级分组下对应质检项为加分会话数、扣分会话数, level=2



-- 统计维度: 天/平台/主账号/子账号/质检标准
-- 度量: 扣分会话数, 加分会话数, 质检会话数

-- 统计维度: 天/平台/主账号/子账号/质检标准
-- 度量: 扣分会话数, 加分会话数, 会话总量(已存在)
xqc_dim.snick_full_info_all
    关联并统一维度表

xqc_dws.snick_stat_all
    扣分会话数(add_score_dialog_cnt), 
    加分会话数(subtract_score_dialog_cnt), 
    质检标准id(qc_norm_id)

1. 关联 ods.xinghuan_qc_norm_relate_all 和 ods.xinghuan_employee_snick_all 表, 获取每个质检表准下的子账号, 以及对应的质检标准id, 
筛选质检标准id不为空的记录

2. dwd.xdqc_dialog 查询当天/平台/主账号/子账号, 并通过子账号关联 表1, 获取对应质检标准id

3. 统计当天/平台/主账号/子账号下的质检标准, 扣分会话数, 加分会话数, 质检会话数



