-- BG实时概况(会话总量+告警分布+日环比+中高级告警比例+告警完结率) -- PS: 对于BG部门新增/更名的问题,由于所有统计都是下钻到了店铺维度,因此即使BG变更,其环比计算依旧不受影响
WITH (
    SELECT toYYYYMMDD(today())
) AS today,
(
    SELECT toYYYYMMDD(yesterday())
) AS yesterday
SELECT -- BG部门维度聚合统计
    bg_name,
    sum(
        if(
            isNull(snick_today_dialog_cnt),
            0,
            snick_today_dialog_cnt
        )
    ) AS bg_today_dialog_cnt,
    -- BG当天当前的会话总量
    sum(
        if(
            isNull(snick_yesterday_dialog_cnt),
            0,
            snick_yesterday_dialog_cnt
        )
    ) AS bg_yesterday_dialog_cnt,
    -- BG昨天同时刻的会话总量
    if(
        bg_yesterday_dialog_cnt != 0,
        round(
            sum(diff_dialog_cnt) / bg_yesterday_dialog_cnt * 100,
            1
        ),
        0.0
    ) AS bg_dialog_relative_ratio,
    -- BG会话总量日环比(秒级)
    sum(
        if(
            isNull(snick_today_level_1_cnt),
            0,
            snick_today_level_1_cnt
        )
    ) AS bg_today_level_1_cnt,
    -- BG当天当前初级告警总量 -- BG告警分布
    sum(
        if(
            isNull(snick_today_level_2_cnt),
            0,
            snick_today_level_2_cnt
        )
    ) AS bg_today_level_2_cnt,
    -- BG当天当前中级告警总量 -- BG告警分布
    sum(
        if(
            isNull(snick_today_level_3_cnt),
            0,
            snick_today_level_3_cnt
        )
    ) AS bg_today_level_3_cnt,
    -- BG当天当前高级告警总量 -- BG告警分布
    (
        bg_today_level_1_cnt + bg_today_level_2_cnt + bg_today_level_3_cnt
    ) AS bg_today_warning_cnt,
    -- BG当天当前告警总量
    if(
        bg_today_dialog_cnt != 0,
        round(
            (bg_today_level_2_cnt + bg_today_level_3_cnt) / bg_today_dialog_cnt * 100,
            1
        ),
        0.0
    ) AS bg_level_2_3_ratio,
    -- BG当天当前中高级告警比例
    sum(
        if(
            isNull(snick_today_level_2_finished_cnt),
            0,
            snick_today_level_2_finished_cnt
        )
    ) AS bg_today_level_2_finished_cnt,
    -- BG当天当前已完结中级告警总量
    sum(
        if(
            isNull(snick_today_level_3_finished_cnt),
            0,
            snick_today_level_3_finished_cnt
        )
    ) AS bg_today_level_3_finished_cnt,
    -- BG当天当前已完结高级告警总量
    if(
        bg_today_level_2_cnt != 0,
        round(
            bg_today_level_2_finished_cnt / bg_today_level_2_cnt * 100,
            1
        ),
        0.0
    ) AS bg_level_2_finished_ratio,
    -- BG当天当前中级告警完结率
    if(
        bg_today_level_3_cnt != 0,
        round(
            bg_today_level_3_finished_cnt / bg_today_level_3_cnt * 100,
            1
        ),
        0.0
    ) AS bg_level_3_finished_ratio -- BG当天当前高级告警完结率
FROM (
        -- bg_name--shop_id
        SELECT bg_name,
            shop_id
        FROM (
                -- bg_name--bg_id SELECT department_name AS bg_name, department_id AS bg_id FROM xqc_dim.group_all WHERE company_id = '6131e6554524490001fc6825' AND level = 1 AND is_shop = 'False' ) GLOBAL LEFT JOIN ( -- bg_id--shop_id SELECT DISTINCT parent_department_path[1] AS bg_id, department_id AS shop_id FROM xqc_dim.group_all WHERE company_id = '6131e6554524490001fc6825' AND is_shop = 'True' ) USING bg_id ) AS bg_shop GLOBAL LEFT JOIN ( -- 子账号维度聚合统计 -- shop_id--snick--statistic SELECT * FROM ( -- 子账号维度今天和昨天会话数据聚合统计 -- shop_id--snick--statistic SELECT shop_id, snick, sum(day = yesterday AND substr(`time`,11)<=substr(toString(now()),11)) AS snick_yesterday_dialog_cnt, -- 子账号昨天同时刻会话总量 sum(day = today) AS snick_today_dialog_cnt, -- 子账号当天当前会话总量 (snick_today_dialog_cnt - snick_yesterday_dialog_cnt) AS diff_dialog_cnt -- 子账号当天和昨天同时刻会话总量差值(后续上卷聚合) FROM xqc_ods.dialog_all WHERE day BETWEEN yesterday AND today -- 组织架构包含店铺 AND shop_id GLOBAL IN ( SELECT department_id AS shop_id FROM xqc_dim.group_all WHERE company_id = '6131e6554524490001fc6825' AND is_shop = 'True' -- AND platform = 'jd' ) -- 权限隔离 AND ( shop_id IN splitByChar(',','61d6a38716bbc36cb34dfd4c,61d6a38716bbc36cb34dfd56,61d6a38716bbc36cb34dfd52,61c193262ba76f001d769b90,6170ddb2abefdb000c773b0a,616d2b651ffab50014d6f922,6172894009841b000fafffc9,616d49b11ffab50016d6fa49,616fccff269ebf000e1b88b0,616e207da08ae900109dcf33,616e1b70abefdb0010773a23,616d282d1ffab50012d6f485,61d6a38716bbc36cb34dfd46,61d6a38716bbc36cb34dfd48,61d6a38716bbc36cb34dfd58,61d6a38716bbc36cb34dfd54,61d6a38716bbc36cb34dfd50,61d6a38716bbc36cb34dfd4a,61d6a38716bbc36cb34dfd4e,616face4a08ae9000e9dd0a9,616f7c6d09841b000eaff41e,61c94f4f6383be001deb8e21,6139c3c96ebd17000e94b5b5,6139e720fb530f0010c19481,613af5f56ebd17000f942ca2,6131c3766ebd17000a93c0cd,614ae633fb530f0010c1b33f,5cd268e42bf9a8000f9301d7,614c21b16ebd170010947761,6139c118e16787000fb8a1cf,618ca3649416a3001c5f413d') OR snick IN splitByChar(',','') ) GROUP BY shop_id, snick ) AS snick_dialog_stat -- 各个子账号两天内的会话统计 GLOBAL LEFT JOIN ( -- 子账号维度当天告警数据聚合统计 -- shop_id--snick--statistic SELECT shop_id, snick, count(1) AS snick_today_warning_cnt, -- 子账号当天当前的告警总量 sum(`level` = 1) AS snick_today_level_1_cnt, -- 子账号当天初级告警量 sum(`level` = 2) AS snick_today_level_2_cnt, -- 子账号当天中级告警量 sum(`level` = 3) AS snick_today_level_3_cnt, -- 子账号当天高级告警量 sum(`level` = 2 AND is_finished = 'True') AS snick_today_level_2_finished_cnt, -- 子账号当天中级已处理告警量 sum(`level` = 3 AND is_finished = 'True') AS snick_today_level_3_finished_cnt -- 子账号当天高级已处理告警量 FROM xqc_ods.alert_all WHERE day = today -- 组织架构包含店铺 AND shop_id GLOBAL IN ( SELECT department_id AS shop_id FROM xqc_dim.group_all WHERE company_id = '6131e6554524490001fc6825' AND is_shop = 'True' -- AND platform = 'jd' ) -- 权限隔离 AND ( shop_id IN splitByChar(',','61d6a38716bbc36cb34dfd4c,61d6a38716bbc36cb34dfd56,61d6a38716bbc36cb34dfd52,61c193262ba76f001d769b90,6170ddb2abefdb000c773b0a,616d2b651ffab50014d6f922,6172894009841b000fafffc9,616d49b11ffab50016d6fa49,616fccff269ebf000e1b88b0,616e207da08ae900109dcf33,616e1b70abefdb0010773a23,616d282d1ffab50012d6f485,61d6a38716bbc36cb34dfd46,61d6a38716bbc36cb34dfd48,61d6a38716bbc36cb34dfd58,61d6a38716bbc36cb34dfd54,61d6a38716bbc36cb34dfd50,61d6a38716bbc36cb34dfd4a,61d6a38716bbc36cb34dfd4e,616face4a08ae9000e9dd0a9,616f7c6d09841b000eaff41e,61c94f4f6383be001deb8e21,6139c3c96ebd17000e94b5b5,6139e720fb530f0010c19481,613af5f56ebd17000f942ca2,6131c3766ebd17000a93c0cd,614ae633fb530f0010c1b33f,5cd268e42bf9a8000f9301d7,614c21b16ebd170010947761,6139c118e16787000fb8a1cf,618ca3649416a3001c5f413d') OR snick IN splitByChar(',','') ) -- 筛选新版本告警 AND `level` IN [1,2,3] GROUP BY shop_id, snick ) AS snick_warning_stat -- 各个子账号当天当前的告警统计 USING shop_id, snick ) AS shop_snick_stat USING shop_id GROUP BY bg_name ORDER BY bg_name ASC LIMIT 8 -- 因为前端UI长度限制,在查询时写死限制8条记录-- trace:40ff107b9a22a7b1fadec0c3f509b1f5