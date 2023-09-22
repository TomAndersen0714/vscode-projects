SELECT log.app_id,
    application.name AS agent,
    count(*) AS 应答数
FROM knowledge_log AS log
    LEFT JOIN application ON log.app_id = application.id
WHERE log.app_id IN (
        SELECT id
        FROM application
        WHERE tenant_id = 'q-62331bf521774b174b35f079'
    )
GROUP BY log.app_id,
    application.name;