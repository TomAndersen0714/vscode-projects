-- 总体会话概况-分时监控量
-- PS: 昨日的hour固定长度为23,今日的hour根据当前时间范围进行展示
WITH 
( SELECT toYYYYMMDD(today()) ) AS today,
( SELECT toYYYYMMDD(yesterday()) ) AS yesterday
SELECT
    if(day=yesterday,'昨日','今日') AS d,
    hour,
    cnt
FROM (
    SELECT
        day,
        hour,
        sum(id!='') AS cnt -- 分时监控量
    FROM xqc_ods.dialog_all
    WHERE day BETWEEN yesterday AND today

    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        AND platform = '{{ platform=tb }}'
    )
    GROUP BY day,hour
)
GLOBAL RIGHT JOIN (
    -- 固定时间轴, 避免空缺
    SELECT yesterday AS day, arrayJoin(range(0,24,1)) AS hour
    UNION ALL
    SELECT today AS day, arrayJoin(range(0,toHour(now())+1,1)) AS hour
)
USING day, hour
ORDER BY day ASC,hour ASC