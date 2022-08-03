-- 数据导出-明察质检-历史会话消息查询-下拉框-AI质检项-客服情绪项
SELECT
    CONCAT(tag_name, '//', tag_id) AS tag_name_id
FROM (
    SELECT
        qc_rule_type AS tag_type,
        qc_rule_id AS tag_id,
        qc_rule_name AS tag_name
    FROM xqc_dim.qc_rule_constant_all
    WHERE day = toYYYYMMDD(yesterday())
    UNION ALL
    SELECT
        'ai_s_emotion' AS tag_type,
        '0' AS tag_id,
        '中性' AS tag_name
)
WHERE tag_type = 'ai_s_emotion'
ORDER BY tag_name_id COLLATE 'zh'