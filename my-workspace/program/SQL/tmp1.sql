-- 宝尊质检报表-店铺
SELECT
    day AS `日期`,
    bg_name,
    bu_name,
    shop_name,
    if(bg_name='Z','---',bg_name) AS BG,
    if(bu_name='Z','---',bu_name) AS BU,
    -- shop_id,
    CASE
        WHEN platform='tb' THEN '淘宝'
        WHEN platform='jd' THEN '京东'
        WHEN platform='ks' THEN '快手'
        WHEN platform='dy' THEN '抖音'
        WHEN platform='pdd' THEN '拼多多'
        WHEN platform='open' THEN '开放平台'
        WHEN platform='Z' THEN '---'
        ELSE platform
    END AS `平台`,
    if(shop_name='Z','汇总',shop_name) AS `店铺`,
    dialog_cnt AS `会话总量`,
    snick_cnt AS `子账号数量`,
    alert_cnt AS `告警总量`,
    finished_alert_cnt AS `告警完结总量`,
    alert_dialog_cnt AS `告警会话量`,
    auto_remind_alert_cnt AS `自动提醒告警量`,
    level_3_alert_cnt AS `高级告警量`,
    level_2_alert_cnt AS `中级告警量`,
    level_1_alert_cnt AS `初级告警量`,
    level_3_alert_pct AS `高级告警比例`,
    level_2_alert_pct AS `中级告警比例`,
    level_1_alert_pct AS `初级告警比例`,
    level_3_finished_alert_cnt AS `高级告警完结量`,
    level_2_finished_alert_cnt AS `中级告警完结量`,
    level_1_finished_alert_cnt AS `初级告警完结量`,
    level_3_finished_alert_pct AS `高级告警完结率`,
    level_2_finished_alert_pct AS `中级告警完结率`,
    level_1_finished_alert_pct AS `初级告警完结率`,
    finished_alert_pct AS `告警完结率`,
    alert_elapsed_min_time_avg AS `告警平均完结时长`,
    alert_dialog_pct AS `告警会话比例`,
    dialog_remind_cnt_avg AS `会话平均提醒次数`
FROM (
    -- 按天/BG/BU/店铺统计
    SELECT
        day, bg_name, bu_name, platform, shop_id, shop_name,
        dialog_cnt, -- 会话总量
        snick_cnt, -- 子账号数量
        alert_cnt, -- 告警总量
        finished_alert_cnt, -- 告警完结总量
        alert_dialog_cnt, -- 告警会话量
        auto_remind_alert_cnt, -- 自动触发提醒告警量
        level_3_alert_cnt, -- 高级告警量
        level_2_alert_cnt, -- 中级告警量
        level_1_alert_cnt, -- 初级告警量
        CONCAT(if(alert_cnt!=0, toString(round(level_3_alert_cnt/alert_cnt*100, 2)), '0'),'%') AS level_3_alert_pct, -- 高级告警比例
        CONCAT(if(alert_cnt!=0, toString(round(level_2_alert_cnt/alert_cnt*100, 2)), '0'),'%') AS level_2_alert_pct, -- 中级告警比例
        CONCAT(if(alert_cnt!=0, toString(round(level_1_alert_cnt/alert_cnt*100, 2)), '0'),'%') AS level_1_alert_pct, -- 初级告警比例
        level_3_finished_alert_cnt, -- 高级告警完结量
        level_2_finished_alert_cnt, -- 中级告警完结量
        level_1_finished_alert_cnt, -- 初级告警完结量
        CONCAT(if(level_3_alert_cnt!=0, toString(round(level_3_finished_alert_cnt/level_3_alert_cnt*100, 2)), '0'),'%') AS level_3_finished_alert_pct, -- 高级告警完结率
        CONCAT(if(level_2_alert_cnt!=0, toString(round(level_2_finished_alert_cnt/level_2_alert_cnt*100, 2)), '0'),'%') AS level_2_finished_alert_pct, -- 中级告警完结率
        CONCAT(if(level_1_alert_cnt!=0, toString(round(level_1_finished_alert_cnt/level_1_alert_cnt*100, 2)), '0'),'%') AS level_1_finished_alert_pct, -- 初级告警完结率
        CONCAT(if(alert_cnt!=0, toString(round(finished_alert_cnt/alert_cnt*100, 2)), '0'),'%') AS finished_alert_pct, -- 告警完结率
        if(finished_alert_cnt!=0, toString(round(alert_elapsed_min_time_sum/finished_alert_cnt, 2)), '0') AS alert_elapsed_min_time_avg, -- 告警平均完结时长
        CONCAT(if(dialog_cnt!=0, toString(round(alert_dialog_cnt/dialog_cnt*100, 2)), '0'),'%') AS alert_dialog_pct, -- 告警会话比例
        CONCAT(if(dialog_cnt!=0, toString(round(auto_remind_cnt/dialog_cnt*100, 2)), '0'),'%') AS dialog_remind_cnt_avg -- 平均会话提醒次数
    FROM (
        -- 获取组织架构维度数据, 天/BG/BU/店铺, 仅展示有店铺的维度数据
        SELECT
            day,
            bg_name, bu_name, platform, shop_id, shop_name
        FROM (
            SELECT
                bg_name, bu_name, platform, shop_id, shop_name
            FROM (
                SELECT DISTINCT
                    bg_id, bu_id, bu_name, platform, shop_id, shop_name
                FROM (
                    SELECT DISTINCT
                        parent_department_path[1] AS bg_id,
                        parent_department_path[2] AS bu_id,
                        platform,
                        department_id AS shop_id,
                        department_name AS shop_name
                    FROM xqc_dim.group_all
                    WHERE company_id='6131e6554524490001fc6825'
                    AND is_shop = 'True'
                ) AS shop_info
                GLOBAL LEFT JOIN (
                    SELECT DISTINCT
                        parent_department_path[1] AS bg_id,
                        department_id AS bu_id,
                        department_name AS bu_name
                    FROM xqc_dim.group_all
                    WHERE company_id='6131e6554524490001fc6825'
                    AND level = 2
                    AND is_shop = 'False'
                ) AS bu_info
                USING(bg_id, bu_id)
                WHERE 
                -- 下拉框-BG
                (
                    ''='' 
                    OR
                    bg_id IN splitByChar(',','')
                )
                -- 下拉框-BU
                AND (
                    ''='' 
                    OR 
                    bu_id IN splitByChar(',','')
                )
                -- 下拉框-店铺
                AND (
                    ''='' 
                    OR 
                    shop_id IN splitByChar(',','')
                )
            ) AS bg_bu_shop_info
            GLOBAL LEFT JOIN (
                SELECT DISTINCT
                    department_id AS bg_id,
                    department_name AS bg_name
                FROM xqc_dim.group_all
                WHERE company_id='6131e6554524490001fc6825'
                AND level = 1
                AND is_shop = 'False'
            ) AS bg_info
            USING(bg_id)
        ) AS dim_info
        GLOBAL CROSS JOIN (
            SELECT
            arrayJoin(
                arrayMap(x->toYYYYMMDD(toDate(x)),
                range(toUInt32(toDate('2022-01-18')), toUInt32(toDate('2022-01-24') + 1), 1))
            ) AS day
        ) AS day
        ORDER BY day,bg_name,bu_name,shop_name
    ) dim_info
    GLOBAL LEFT JOIN (
        SELECT *
        FROM (
            SELECT *
            FROM (
                -- 会话类指标统计-天/店铺维度
                SELECT
                    day,
                    shop_id,
                    COUNT(1) AS dialog_cnt, -- 会话总量
                    COUNT(DISTINCT snick) AS snick_cnt -- 子账号数量
                FROM xqc_ods.dialog_all
                WHERE shop_id IN (
                    -- 已订阅店铺
                    SELECT tenant_id AS shop_id
                    FROM xqc_dim.company_tenant
                    WHERE company_id = '6131e6554524490001fc6825'
                )
                AND day BETWEEN toYYYYMMDD(toDate('2022-01-18')) AND toYYYYMMDD(toDate('2022-01-24'))
                GROUP BY day, shop_id
            ) AS day_shop_dialog_info
            GLOBAL FULL OUTER JOIN (
                -- 告警类指标统计-天/店铺维度
                SELECT
                    day,
                    shop_id,
                    COUNT(1) AS alert_cnt, -- 总告警次数
                    COUNT(DISTINCT dialog_id) AS alert_dialog_cnt, -- 告警会话量
                    SUM(level=3) AS level_3_alert_cnt, -- 高级告警量
                    SUM(level=2) AS level_2_alert_cnt, -- 中级告警量
                    SUM(level=1) AS level_1_alert_cnt, -- 初级告警量
                    SUM(level=3 AND is_finished = 'True') AS level_3_finished_alert_cnt, -- 高级告警完结量
                    SUM(level=2 AND is_finished = 'True') AS level_2_finished_alert_cnt, -- 中级告警完结量
                    SUM(level=1 AND is_finished = 'True') AS level_1_finished_alert_cnt, -- 初级告警完结量
                    level_1_finished_alert_cnt+level_2_finished_alert_cnt+level_3_finished_alert_cnt AS  finished_alert_cnt, -- 告警完结量
                    SUM(
                        toInt64(if(
                            is_finished='True',
                            round((parseDateTimeBestEffort(if(finish_time!='',finish_time,toString(now()))) - parseDateTimeBestEffort(time))/60),
                            round((now() - parseDateTimeBestEffort(time))/60)
                        ))
                    ) AS alert_elapsed_min_time_sum -- 告警完结总时长(min)
                FROM xqc_ods.alert_all FINAL
                WHERE shop_id IN (
                    -- 已订阅店铺
                    SELECT tenant_id AS shop_id
                    FROM xqc_dim.company_tenant
                    WHERE company_id = '6131e6554524490001fc6825'
                )
                AND day BETWEEN toYYYYMMDD(toDate('2022-01-18')) AND toYYYYMMDD(toDate('2022-01-24'))
                GROUP BY day, shop_id
            ) AS day_shop_alert_info
            USING(day, shop_id)
        ) AS day_shop_dialog_alert_info
        GLOBAL FULL OUTER JOIN (
            -- 告警提醒类指标统计-天/店铺维度
            SELECT
                day,
                shop_id,
                COUNT(DISTINCT alert_id) AS auto_remind_alert_cnt, -- 自动触发提醒告警量(触发了自动实时提醒的告警数)
                COUNT(DISTINCT id)  AS auto_remind_cnt-- 自动发送实时提醒次数
            FROM xqc_ods.alert_remind_all
            WHERE shop_id IN (
                -- 已订阅店铺
                SELECT tenant_id AS shop_id
                FROM xqc_dim.company_tenant
                WHERE company_id = '6131e6554524490001fc6825'
            )
            AND day BETWEEN toYYYYMMDD(toDate('2022-01-18')) AND toYYYYMMDD(toDate('2022-01-24'))
            AND source = 1 -- 自动触发
            AND notify_type = 1 -- 实时触发
            GROUP BY day, shop_id
        ) AS day_shop_remind_info
        USING(day, shop_id)
    ) AS day_shop_stat_info
    USING(day, shop_id)

    UNION ALL

    -- 按天汇总
    SELECT
        day, 'Z' AS bg_name, 'Z' AS bu_name, 'Z' AS platform, 'Z' AS shop_id, 'Z' AS shop_name,
        dialog_cnt_sum AS dialog_cnt, -- 会话总量
        snick_cnt_sum AS snick_cnt, -- 子账号数量
        alert_cnt_sum AS alert_cnt, -- 告警总量
        finished_alert_cnt_sum AS finished_alert_cnt, -- 告警完结总量
        alert_dialog_cnt_sum AS alert_dialog_cnt, -- 告警会话量
        auto_remind_alert_cnt_sum AS auto_remind_alert_cnt, -- 自动触发提醒告警量
        level_3_alert_cnt_sum AS level_3_alert_cnt, -- 高级告警量
        level_2_alert_cnt_sum AS level_2_alert_cnt, -- 中级告警量
        level_1_alert_cnt_sum AS level_1_alert_cnt, -- 初级告警量
        level_3_alert_pct, -- 高级告警比例
        level_2_alert_pct, -- 中级告警比例
        level_1_alert_pct, -- 初级告警比例
        level_3_finished_alert_cnt_sum AS level_3_finished_alert_cnt, -- 高级告警完结量
        level_2_finished_alert_cnt_sum AS level_2_finished_alert_cnt, -- 中级告警完结量
        level_1_finished_alert_cnt_sum AS level_1_finished_alert_cnt, -- 初级告警完结量
        level_3_finished_alert_pct, -- 高级告警完结率
        level_2_finished_alert_pct, -- 中级告警完结率
        level_1_finished_alert_pct, -- 初级告警完结率
        finished_alert_pct, -- 告警完结率
        alert_elapsed_min_time_avg, -- 告警平均完结时长
        alert_dialog_pct, -- 告警会话比例
        dialog_remind_cnt_avg -- 平均会话提醒次数
    FROM (
        SELECT
            day,
            SUM(dialog_cnt) AS dialog_cnt_sum, -- 会话总量
            SUM(snick_cnt) AS snick_cnt_sum, -- 子账号数量
            SUM(alert_cnt) AS alert_cnt_sum, -- 告警总量
            SUM(alert_dialog_cnt) AS alert_dialog_cnt_sum, -- 告警会话量
            SUM(auto_remind_alert_cnt) AS auto_remind_alert_cnt_sum, -- 自动触发提醒告警量
            SUM(level_3_alert_cnt) AS level_3_alert_cnt_sum, -- 高级告警量
            SUM(level_2_alert_cnt) AS level_2_alert_cnt_sum, -- 中级告警量
            SUM(level_1_alert_cnt) AS level_1_alert_cnt_sum, -- 初级告警量
            SUM(level_3_finished_alert_cnt) AS level_3_finished_alert_cnt_sum, -- 高级告警完结量
            SUM(level_2_finished_alert_cnt) AS level_2_finished_alert_cnt_sum, -- 中级告警完结量
            SUM(level_1_finished_alert_cnt) AS level_1_finished_alert_cnt_sum, -- 初级告警完结量
            SUM(finished_alert_cnt) AS finished_alert_cnt_sum,
            SUM(alert_elapsed_min_time_sum) AS alert_elapsed_min_time_sum_sum,
            SUM(auto_remind_cnt) AS auto_remind_cnt_sum,
            CONCAT(if(alert_cnt_sum!=0, toString(round(level_3_alert_cnt_sum/alert_cnt_sum*100, 2)), '0'),'%') AS level_3_alert_pct, -- 高级告警比例
            CONCAT(if(alert_cnt_sum!=0, toString(round(level_2_alert_cnt_sum/alert_cnt_sum*100, 2)), '0'),'%') AS level_2_alert_pct, -- 中级告警比例
            CONCAT(if(alert_cnt_sum!=0, toString(round(level_1_alert_cnt_sum/alert_cnt_sum*100, 2)), '0'),'%') AS level_1_alert_pct, -- 初级告警比例
            CONCAT(if(level_3_alert_cnt_sum!=0, toString(round(level_3_finished_alert_cnt_sum/level_3_alert_cnt_sum*100, 2)), '0'),'%') AS level_3_finished_alert_pct, -- 高级告警完结率
            CONCAT(if(level_2_alert_cnt_sum!=0, toString(round(level_2_finished_alert_cnt_sum/level_2_alert_cnt_sum*100, 2)), '0'),'%') AS level_2_finished_alert_pct, -- 中级告警完结率
            CONCAT(if(level_1_alert_cnt_sum!=0, toString(round(level_1_finished_alert_cnt_sum/level_1_alert_cnt_sum*100, 2)), '0'),'%') AS level_1_finished_alert_pct, -- 初级告警完结率
            CONCAT(if(alert_cnt_sum!=0, toString(round(finished_alert_cnt_sum/alert_cnt_sum*100, 2)), '0'),'%') AS finished_alert_pct, -- 告警完结率
            if(finished_alert_cnt_sum!=0, toString(round(alert_elapsed_min_time_sum_sum/finished_alert_cnt_sum, 2)), '0') AS alert_elapsed_min_time_avg, -- 告警平均完结时长
            CONCAT(if(dialog_cnt_sum!=0, toString(round(alert_dialog_cnt_sum/dialog_cnt_sum*100, 2)), '0'),'%') AS alert_dialog_pct, -- 告警会话比例
            CONCAT(if(dialog_cnt_sum!=0, toString(round(auto_remind_cnt_sum/dialog_cnt_sum*100, 2)), '0'),'%') AS dialog_remind_cnt_avg -- 平均会话提醒次数
        FROM (
            -- 获取组织架构维度数据, 天/BG/BU/店铺, 仅展示有店铺的维度数据
            SELECT
                day,
                bg_name, bu_name, shop_id, shop_name
            FROM (
                SELECT
                    bg_name, bu_name, shop_id, shop_name
                FROM (
                    SELECT DISTINCT
                        bg_id, bu_id, bu_name, shop_id, shop_name
                    FROM (
                        SELECT DISTINCT
                            parent_department_path[1] AS bg_id,
                            parent_department_path[2] AS bu_id,
                            department_id AS shop_id,
                            department_name AS shop_name
                        FROM xqc_dim.group_all
                        WHERE company_id='6131e6554524490001fc6825'
                        AND is_shop = 'True'
                    ) AS shop_info
                    GLOBAL LEFT JOIN ( 
                        SELECT DISTINCT
                            parent_department_path[1] AS bg_id,
                            department_id AS bu_id,
                            department_name AS bu_name
                        FROM xqc_dim.group_all
                        WHERE company_id='6131e6554524490001fc6825'
                        AND level = 2
                        AND is_shop = 'False'
                    ) AS bu_info
                    USING(bg_id, bu_id)
                    WHERE 
                    -- 下拉框-BG
                    (
                        ''='' 
                        OR
                        bg_id IN splitByChar(',','')
                    )
                    -- 下拉框-BU
                    AND (
                        ''='' 
                        OR 
                        bu_id IN splitByChar(',','')
                    )
                    -- 下拉框-店铺
                    AND (
                        ''='' 
                        OR 
                        shop_id IN splitByChar(',','')
                    )
                ) AS bg_bu_shop_info
                GLOBAL LEFT JOIN (
                    SELECT DISTINCT
                        department_id AS bg_id,
                        department_name AS bg_name
                    FROM xqc_dim.group_all
                    WHERE company_id='6131e6554524490001fc6825'
                    AND level = 1
                    AND is_shop = 'False'
                ) AS bg_info
                USING(bg_id)
            ) AS dim_info
            GLOBAL CROSS JOIN (
                SELECT
                arrayJoin(
                    arrayMap(x->toYYYYMMDD(toDate(x)),
                    range(toUInt32(toDate('2022-01-18')), toUInt32(toDate('2022-01-24') + 1), 1))
                ) AS day
            ) AS day
            ORDER BY day,bg_name,bu_name,shop_name
        ) dim_info
        GLOBAL LEFT JOIN (
            SELECT *
            FROM (
                SELECT *
                FROM (
                    -- 会话类指标统计-天/店铺维度
                    SELECT
                        day,
                        shop_id,
                        COUNT(1) AS dialog_cnt, -- 会话总量
                        COUNT(DISTINCT snick) AS snick_cnt -- 子账号数量
                    FROM xqc_ods.dialog_all
                    WHERE shop_id IN (
                        -- 已订阅店铺
                        SELECT tenant_id AS shop_id
                        FROM xqc_dim.company_tenant
                        WHERE company_id = '6131e6554524490001fc6825'
                    )
                    AND day BETWEEN toYYYYMMDD(toDate('2022-01-18')) AND toYYYYMMDD(toDate('2022-01-24'))
                    GROUP BY day, shop_id
                ) AS day_shop_dialog_info
                GLOBAL FULL OUTER JOIN (
                    -- 告警类指标统计-天/店铺维度
                    SELECT
                        day,
                        shop_id,
                        COUNT(1) AS alert_cnt, -- 总告警次数
                        COUNT(DISTINCT dialog_id) AS alert_dialog_cnt, -- 告警会话量
                        SUM(level=3) AS level_3_alert_cnt, -- 高级告警量
                        SUM(level=2) AS level_2_alert_cnt, -- 中级告警量
                        SUM(level=1) AS level_1_alert_cnt, -- 初级告警量
                        SUM(level=3 AND is_finished = 'True') AS level_3_finished_alert_cnt, -- 高级告警完结量
                        SUM(level=2 AND is_finished = 'True') AS level_2_finished_alert_cnt, -- 中级告警完结量
                        SUM(level=1 AND is_finished = 'True') AS level_1_finished_alert_cnt, -- 初级告警完结量
                        level_1_finished_alert_cnt+level_2_finished_alert_cnt+level_3_finished_alert_cnt AS  finished_alert_cnt, -- 告警完结量
                        SUM(
                            toInt64(if(
                                is_finished='True',
                                round((parseDateTimeBestEffort(if(finish_time!='',finish_time,toString(now()))) - parseDateTimeBestEffort(time))/60),
                                round((now() - parseDateTimeBestEffort(time))/60)
                            ))
                        ) AS alert_elapsed_min_time_sum -- 告警完结总时长(min)
                    FROM xqc_ods.alert_all FINAL
                    WHERE shop_id IN (
                        -- 已订阅店铺
                        SELECT tenant_id AS shop_id
                        FROM xqc_dim.company_tenant
                        WHERE company_id = '6131e6554524490001fc6825'
                    )
                    AND day BETWEEN toYYYYMMDD(toDate('2022-01-18')) AND toYYYYMMDD(toDate('2022-01-24'))
                    GROUP BY day, shop_id
                ) AS day_shop_alert_info
                USING(day, shop_id)
            ) AS day_shop_dialog_alert_info
            GLOBAL FULL OUTER JOIN (
                -- 告警提醒类指标统计-天/店铺维度
                SELECT
                    day,
                    shop_id,
                    COUNT(DISTINCT alert_id) AS auto_remind_alert_cnt, -- 自动触发提醒告警量(触发了自动实时提醒的告警数)
                    COUNT(DISTINCT id)  AS auto_remind_cnt-- 自动发送实时提醒次数
                FROM xqc_ods.alert_remind_all
                WHERE shop_id IN (
                    -- 已订阅店铺
                    SELECT tenant_id AS shop_id
                    FROM xqc_dim.company_tenant
                    WHERE company_id = '6131e6554524490001fc6825'
                )
                AND day BETWEEN toYYYYMMDD(toDate('2022-01-18')) AND toYYYYMMDD(toDate('2022-01-24'))
                AND source = 1 -- 自动触发
                AND notify_type = 1 -- 实时触发
                GROUP BY day, shop_id
            ) AS day_shop_remind_info
            USING(day, shop_id)
        ) AS day_shop_stat_info
        USING(day, shop_id)
        GROUP BY day
    ) AS day_stat_info
) AS stat_info
ORDER BY day, bg_name ASC, bu_name ASC, shop_name ASC COLLATE 'zh_Hans_CN'