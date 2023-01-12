-- 集团实时概况(分时监控量)(改,昨日的hour固定长度为23,今日的hour根据当前时间范围进行展示)
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
    -- 组织架构包含店铺
    AND shop_id GLOBAL IN (
        SELECT department_id AS shop_id
        FROM xqc_dim.group_all
        WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
        AND is_shop = 'True'
        -- AND platform = '{{ platform=jd }}'
    )
    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{ shop_id_list=6139c118e16787000fb8a1cf }}')
            OR
            snick IN splitByChar(',','{{ snick_list=NULL }}')
        )
    GROUP BY day,hour
)
GLOBAL RIGHT JOIN (
    SELECT yesterday AS day, arrayJoin(range(0,24,1)) AS hour
    UNION ALL
    SELECT today AS day, arrayJoin(range(0,toHour(now())+1,1)) AS hour
)
USING day, hour
ORDER BY day ASC,hour ASC