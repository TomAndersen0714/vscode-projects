-- 数据导出-明察质检-历史会话消息查询-下拉框-AI质检项-扣分项
SELECT
    qc_rule_type AS tag_type,
    qc_rule_id AS tag_id,
    qc_rule_name AS tag_name,
    CONCAT(qc_rule_name, '//', qc_rule_id) AS tag_name_id
FROM xqc_dim.qc_rule_constant_all
WHERE day = toYYYYMMDD(yesterday())
AND qc_rule_type = 'ai_abnormal'
ORDER BY tag_name_id COLLATE 'zh'