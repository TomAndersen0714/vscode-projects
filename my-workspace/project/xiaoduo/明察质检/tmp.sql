SELECT
    hostName(),
    *
FROM remote('{{host}}', 'system.query_log')
WHERE toYYYYMMDD(event_date) BETWEEN toYYYYMMDD(toDateTime('{{datetime.start}}')) AND toYYYYMMDD(toDateTime('{{datetime.end}}'))
    AND event_time BETWEEN toDateTime('{{datetime.start}}') AND toDateTime('{{datetime.end}}')
    AND memory_usage >= {{memory_mb_threshold}}*1024*1024
    AND type in [{{type}}]
    AND query ilike '{{query_segment1}}'
ORDER BY {{desc_order_key}} desc
LIMIT {{limit}}