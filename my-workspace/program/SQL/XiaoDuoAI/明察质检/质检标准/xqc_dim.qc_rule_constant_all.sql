-- xqc_dim.qc_rule_constant_local

-- qc_rule_type: 
'ai_abnormal',
'ai_excellent',
'ai_s_emotion',
'ai_c_emotion',
'manual',
'custom'
-- qc_rule_id: 
1~29, 100
-- qc_rule_group_name: 
'未分组-AI',
'未分组-人工',
'未分组-自定义'


-- xqc_dim.qc_rule_constant_local
DROP TABLE xqc_dim.qc_rule_constant_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.qc_rule_constant_local ON CLUSTER cluster_3s_2r
(
    `qc_rule_type` String,
    `qc_rule_id` String,
    `qc_rule_name` String,
    `qc_rule_group_id` String,
    `qc_rule_group_name` String,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (qc_rule_type, qc_rule_id)
SETTINGS storage_policy = 'rr', index_granularity = 8192


-- xqc_dim.qc_rule_constant_all
DROP TABLE xqc_dim.qc_rule_constant_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.qc_rule_constant_all ON CLUSTER cluster_3s_2r
AS xqc_dim.qc_rule_constant_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'qc_rule_constant_local', rand())

-- INSERT INTO
INSERT INTO xqc_dim.qc_rule_constant_all
-- AI扣分质检项
SELECT
    'ai_abnormal' AS qc_rule_type,
    qc_rule_id,
    qc_rule_name,
    '' AS qc_rule_group_id,
    '未分组-AI' AS qc_rule_group_name,
    toYYYYMMDD(yesterday()) AS day
FROM numbers(1)
ARRAY JOIN
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 100]
        AS qc_rule_id,
    ['非客服结束会话', '漏跟进', '快捷短语重复', '生硬拒绝', '欠缺安抚', '答非所问', '单字回复', '单句响应慢', '产品不熟悉', '活动不熟悉', '内部回复慢', '回复严重超时', '撤回人工消息', '单表情回复', '异常撤回', '转接前未有效回复', '超时未回复', '顾客撤回', '前后回复矛盾', '撤回机器人消息', '第三方投诉或曝光', '顾客提及投诉或举报', '差评或要挟差评', '反问/质疑顾客', '违禁词', '客服冷漠讥讽', '顾客怀疑假货', '客服态度消极敷衍', '售后不满意', '疑似非客服结束会话'] 
        AS qc_rule_name
-- AI加分质检项
UNION ALL
SELECT
    'ai_excellent' AS qc_rule_type,
    qc_rule_id,
    qc_rule_name,
    '' AS qc_rule_group_id,
    '未分组-AI' AS qc_rule_group_name,
    toYYYYMMDD(yesterday()) AS day
FROM numbers(1)
ARRAY JOIN
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
        AS qc_rule_id,
    ['需求挖掘', '商品细节解答', '卖点传达', '商品推荐', '退换货理由修改', '主动跟进', '无货挽回', '活动传达', '店铺保障', '催拍催付', '核对地址', '好评引导', '优秀结束语', '试听课跟单']
        AS qc_rule_name

-- AI买家情绪质检项
UNION ALL
SELECT
    'ai_c_emotion' AS qc_rule_type,
    qc_rule_id,
    qc_rule_name,
    '' AS qc_rule_group_id,
    '未分组-AI' AS qc_rule_group_name,
    toYYYYMMDD(yesterday()) AS day
FROM numbers(1)
ARRAY JOIN
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        AS qc_rule_id,
    ['中性', '满意', '感激', '期待', '对客服态度不满', '对发货物流不满', '对产品不满', '其他不满意', '顾客骂人', '对收货少件不满', '想退款退学']
        AS qc_rule_name

-- AI客服情绪质检项
UNION ALL
SELECT
    'ai_s_emotion' AS qc_rule_type,
    qc_rule_id,
    qc_rule_name,
    '' AS qc_rule_group_id,
    '未分组-AI' AS qc_rule_group_name,
    toYYYYMMDD(yesterday()) AS day
FROM numbers(1)
ARRAY JOIN
    [8] AS qc_rule_id,
    ['客服骂人'] AS qc_rule_name