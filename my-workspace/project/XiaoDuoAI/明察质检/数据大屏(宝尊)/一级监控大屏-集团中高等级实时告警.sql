-- 集团中高等级实时告警(集团各等级告警实时分类统计)
WITH ( SELECT toYYYYMMDD(today()) ) AS today
SELECT
    `level`, -- 告警等级
    warning_type, -- 告警项描述
    sum(is_finished = 'False') AS not_finished_cnt, -- 各告警项未处理总量
    sum(1) AS warning_cnt, -- 各告警项总量
    if(warning_cnt!=0, round((warning_cnt-not_finished_cnt)/warning_cnt*100,1), 0.0) AS warning_finished_ratio-- 各告警项完结率
FROM xqc_ods.alert_all FINAL
WHERE day=today
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
AND `level` IN [2,3]
GROUP BY `level`, warning_type
ORDER BY `level` DESC, not_finished_cnt DESC, warning_type DESC