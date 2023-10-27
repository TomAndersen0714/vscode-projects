-- 集团近30日告警趋势
WITH 
( SELECT toYYYYMMDD(today()) ) AS today,
( SELECT toYYYYMMDD(yesterday()) ) AS yesterday,
( SELECT toYYYYMMDD(today()-30) ) AS month_ago
SELECT
    concat(substr(toString(day),5,2),'/',substr(toString(day),7,2))  as d,
    dialog_cnt, -- 每日会话总量
    level_2_3_sum, -- 每日告警总量(中高级)
    if(dialog_cnt!=0, round(level_2_3_sum/dialog_cnt*100,1), 0.0) AS level_2_3_ratio-- 每日告警比例(中高级)
FROM (
    SELECT day,
        COUNT(1) AS dialog_cnt
    FROM xqc_ods.dialog_all
    WHERE day BETWEEN month_ago AND today
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
    GROUP BY day
) AS dialog_cnt_daily
GLOBAL LEFT JOIN (
    SELECT 
        day,
        sum(level=2) AS level_2_cnt, -- 中级告警总量
        sum(level=3) AS level_3_cnt, -- 高级告警总量
        (level_2_cnt + level_3_cnt) AS level_2_3_sum -- 中高级告警总和
    FROM (
        SELECT DISTINCT
            day, id, level
        FROM xqc_ods.alert_all FINAL
        PREWHERE day BETWEEN month_ago AND today
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
        -- 筛选新版本告警
        AND `level` IN [1,2,3]
    )
    GROUP BY day
) AS level_2_3_sum_daily
USING day
ORDER BY day ASC