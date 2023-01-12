-- 产品数据-明察-页面使用情况
-- 淘宝: impala
WITH t1 AS (
    SELECT DISTINCT url,
        marked_content
    FROM dim.burying_point_marked
    WHERE event = '$pageview'
        AND app_id = 'xd001'
),
t2 AS (
    SELECT url_path,
        distinct_id,
        count(1) AS pv,
        count(DISTINCT distinct_id) AS `登录账户`
    FROM xd_data.web_log
    WHERE `day` <= cast(
            replace(to_date('{{ day.end=yesterday }}'), '-', '') AS int
        )
        AND `day` >= cast(
            replace(to_date('{{ day.start=month_ago }}'), '-', '') AS int
        )
        AND event = '$pageview'
        AND url LIKE '%xh-mc.xiaoduoai.com/%'
        AND app_id IN ('xd001', 'xd023')
        AND length(distinct_id) <= 24
        AND udfs.getvalueformap('people', `properties`, '') = ''
    GROUP BY 1,
        2
)
SELECT t2.*,
    t1.marked_content AS `页面`
FROM t2
    JOIN t1 ON t2.url_path = t1.url
WHERE distinct_id NOT IN ('测试', '测试:测试')
ORDER BY pv DESC