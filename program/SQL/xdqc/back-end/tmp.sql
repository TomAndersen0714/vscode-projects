WITH (
    SELECT toDateTime('{{start_datetime}}')
) AS start_datetime,
(
    SELECT toDateTime('{{end_datetime}}')
) AS end_datetime
select *
from system.query_log
where toYYYYMMDD(event_date) BETWEEN toYYYYMMDD(start_datetime) AND toYYYYMMDD(end_datetime)
    and event_time BETWEEN start_datetime AND end_datetime
    and memory_usage >= {{memory_bytes_threshold}}
order by memory_usage desc
limit 10
