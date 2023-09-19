select
    hostName(),
    *
from remote('{{host}}', 'system.query_log')
where toYYYYMMDD(event_date) BETWEEN toYYYYMMDD(toDateTime('{{datetime.start}}')) AND toYYYYMMDD(toDateTime('{{datetime.end}}'))
    and event_time BETWEEN toDateTime('{{datetime.start}}') AND toDateTime('{{datetime.end}}')
    and memory_usage >= {{memory_mb_threshold}}*1024*1024
    and type in [{{type}}]
    and query like '%{{query}}%'
order by ({{desc_order_key}}) desc
limit {{limit}}