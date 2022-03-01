-- 质检报表-平台
-- 统计维度: 平台, 下钻维度路径: 平台/店铺/子账号分组/子账号/会话
-- PS: 人工质检和自定义质检, 也可以从子账号维度聚合使用groupArray, 后续可以继续
-- 使用sumMap(key_arr_column), flatten(value_arr_column))+group by按需向上卷积

SELECT
    CASE
        WHEN platform='tb' THEN '淘宝'
        WHEN platform='jd' THEN '京东'
        WHEN platform='ks' THEN '快手'
        WHEN platform='dy' THEN '抖音'
        WHEN platform='pdd' THEN '拼多多'
        WHEN platform='open' THEN '开放平台'
        ELSE platform
    END AS `平台`,
    dialog_stat_ai_check_info.*,
    arrayStringConcat(arrayMap(x->toString(x),human_customize_check_info.human_check_tag_name_arr),'$$') AS `人工质检标签`,
    arrayStringConcat(arrayMap(x->toString(x),human_customize_check_info.human_check_tag_cnt_arr),'$$') AS `人工质检触发次数`,
    arrayStringConcat(arrayMap(x->toString(x),human_customize_check_info.customize_check_tag_name_arr),'$$') AS `自定义质检标签`,
    arrayStringConcat(arrayMap(x->toString(x),human_customize_check_info.customize_check_tag_cnt_arr),'$$') AS `自定义质检触发次数`
FROM (
    -- 质检结果总览+AI质检结果
    SELECT
        platform,
        count(distinct seller_nick) AS `店铺数`,
        count(distinct snick) AS `客服人数`,
        sum(dialog_cnt) AS `总会话量`,
        round((`总会话量`*100 + sum(score_add)- sum(score))/`总会话量`,2) AS `平均分`,
        -- AI质检
        `总会话量` AS `AI质检量`,
        sum(abnormal_dialog_cnt) AS `AI异常会话量`,
        sum(ai_score) AS `AI扣分分值`,
        concat(toString(round((`AI异常会话量` * 100 / `总会话量`), 2)),'%') AS `AI扣分会话比例`,
        sum(excellents_dialog_cnt) AS `AI加分会话量`,
        sum(ai_score_add) AS `AI加分分值`,
        concat(toString(round((`AI加分会话量` * 100 / `总会话量`), 2)),'%') AS `AI加分会话比例`,
        -- 人工质检
        round((0.9604 * `总会话量`) /(0.0025 * `总会话量` + 0.9604), 0) as `建议抽检量`,
        sum(mark_dialog_cnt) AS `人工抽检量`,
        concat(toString(round((`人工抽检量` * 100 / `总会话量`), 2)),'%') as `抽检比例`,

        sum(tag_score_dialog_cnt) AS `人工扣分会话量`,
        sum(mark_score) AS `人工扣分分值`,
        concat(toString(round((`人工扣分会话量` * 100 / `总会话量`), 2)),'%') AS `人工扣分会话比例`,
        sum(tag_score_add_dialog_cnt) `人工加分会话量`,
        sum(mark_score_add) AS `人工加分分值`,
        concat(toString(round((`人工加分会话量` * 100 / `总会话量`), 2)),'%') AS `人工加分会话比例`,
        -- 自定义质检
        sum(rule_dialog_cnt) AS `自定义扣分会话量`,
        sum(rule_score) AS `自定义扣分分值`,
        concat(toString(round((`自定义扣分会话量` * 100 / `总会话量`), 2)),'%') AS `自定义扣分会话比例`,
        sum(rule_add_dialog_cnt) AS `自定义加分会话量`,
        sum(rule_score_add) AS `自定义加分分值`,
        concat(toString(round((`自定义加分会话量` * 100 / `总会话量`), 2)),'%') AS `自定义加分会话比例`,
        -- AI质检项
        sum(abnormal_type_1_cnt) AS `非客服结束会话`,
        sum(abnormal_type_2_cnt) AS `漏跟进`,
        sum(abnormal_type_3_cnt) AS `快捷短语重复`,
        sum(abnormal_type_4_cnt) AS `生硬拒绝`,
        sum(abnormal_type_5_cnt) AS `欠缺安抚`,
        sum(abnormal_type_6_cnt) AS `答非所问`,
        sum(abnormal_type_7_cnt) AS `单字回复`,
        sum(abnormal_type_8_cnt) AS `单句响应慢`,
        sum(abnormal_type_9_cnt) AS `产品不熟悉`,
        sum(abnormal_type_10_cnt) AS `活动不熟悉`,
        sum(abnormal_type_11_cnt) AS `内部回复慢`,
        sum(abnormal_type_12_cnt) AS `回复严重超时`,
        sum(abnormal_type_13_cnt) AS `撤回人工消息`,
        sum(abnormal_type_14_cnt) AS `单表情回复`,
        sum(abnormal_type_15_cnt) AS `异常撤回`,
        sum(abnormal_type_16_cnt) AS `转接前未有效回复`,
        sum(abnormal_type_17_cnt) AS `超时未回复`,
        sum(abnormal_type_18_cnt) AS `顾客撤回`,
        sum(abnormal_type_19_cnt) AS `前后回复矛盾`,
        sum(abnormal_type_20_cnt) AS `撤回机器人消息`,
        sum(abnormal_type_21_cnt) AS `第三方投诉或曝光`,
        sum(abnormal_type_22_cnt) AS `顾客提及投诉或举报`,
        sum(abnormal_type_23_cnt) AS `差评或要挟差评`,
        sum(abnormal_type_24_cnt) AS `反问/质疑顾客`,
        sum(abnormal_type_25_cnt) AS `违禁词`,
        sum(abnormal_type_26_cnt) AS `客服冷漠讥讽`,
        sum(abnormal_type_27_cnt) AS `顾客怀疑假货`,
        sum(abnormal_type_28_cnt) AS `客服态度消极敷衍`,
        sum(abnormal_type_29_cnt) AS `售后不满意`,
        sum(excellent_type_1_cnt) AS `需求挖掘`,
        sum(excellent_type_2_cnt) AS `商品细节解答`,
        sum(excellent_type_3_cnt) AS `卖点传达`,
        sum(excellent_type_4_cnt) AS `商品推荐`,
        sum(excellent_type_5_cnt) AS `退换货理由修改`,
        sum(excellent_type_6_cnt) AS `主动跟进`,
        sum(excellent_type_7_cnt) AS `无货挽回`,
        sum(excellent_type_8_cnt) AS `活动传达`,
        sum(excellent_type_9_cnt) AS `店铺保障`,
        sum(excellent_type_10_cnt) AS `催拍催付`,
        sum(excellent_type_11_cnt) AS `核对地址`,
        sum(excellent_type_12_cnt) AS `好评引导`,
        sum(excellent_type_13_cnt) AS `优秀结束语`,
        sum(c_emotion_type_1_cnt) AS `满意`,
        sum(c_emotion_type_2_cnt) AS `感激`,
        sum(c_emotion_type_3_cnt) AS `期待`,
        sum(c_emotion_type_4_cnt) AS `对客服态度不满`,
        sum(c_emotion_type_5_cnt) AS `对发货物流不满`,
        sum(c_emotion_type_6_cnt) AS `对产品不满`,
        sum(c_emotion_type_7_cnt) AS `其他不满意`,
        sum(c_emotion_type_8_cnt) AS `顾客骂人`,
        sum(c_emotion_type_9_cnt) AS `对收货少件不满`,
        sum(s_emotion_type_8_cnt) AS `客服骂人`
    FROM (
        SELECT *
        FROM (
            -- 质检结果总览-子账号维度统计
            SELECT
                platform,
                seller_nick,
                snick,
                COUNT(1) AS dialog_cnt,
                sum(score) AS score,
                sum(score_add) AS score_add,
                sum(mark_score) AS mark_score,
                sum(mark_score_add) AS mark_score_add,
                sum(arraySum(arrayMap((x,y)->x*y,rule_stats_score,rule_stats_count))) AS rule_score,
                sum(arraySum(arrayMap((x,y)->x*y,rule_add_stats_score,rule_add_stats_count))) AS rule_score_add,
                score - mark_score - rule_score AS ai_score,
                score_add - mark_score_add - rule_score_add AS ai_score_add,
                sum(arraySum(abnormals_count)!=0) AS abnormal_dialog_cnt,
                sum(arraySum(excellents_count)!=0) AS excellents_dialog_cnt,
                sum(length(mark_ids)!=0) AS mark_dialog_cnt,
                sum(length(tag_score_stats_id)!=0) AS tag_score_dialog_cnt,
                sum(length(tag_score_add_stats_id)!=0) AS tag_score_add_dialog_cnt,
                sum(length(rule_stats_id)!=0) AS rule_dialog_cnt,
                sum(length(rule_add_stats_id)!=0) AS rule_add_dialog_cnt
            FROM dwd.xdqc_dialog_all
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
            AND platform = 'tb'
            AND snick GLOBAL IN (
                -- 获取最新版本的维度数据(T+1)
                SELECT distinct snick
                FROM ods.xinghuan_employee_snick_all
                WHERE day = toYYYYMMDD(yesterday())
                AND platform = 'tb'
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            )
            GROUP BY platform, seller_nick, snick
        ) AS stat_info
        GLOBAL FULL OUTER JOIN (
            -- AI质检结果
            SELECT *
            FROM (
                -- AI质检-子账号维度加分扣分行为质检结果
                SELECT *
                FROM (
                    -- AI质检-子账号维度扣分质检项触发次数统计
                    SELECT
                        platform,
                        seller_nick,
                        snick,
                        sumIf(abnormal_cnt, abnormal_type=1) AS abnormal_type_1_cnt,
                        sumIf(abnormal_cnt, abnormal_type=2) AS abnormal_type_2_cnt,
                        sumIf(abnormal_cnt, abnormal_type=3) AS abnormal_type_3_cnt,
                        sumIf(abnormal_cnt, abnormal_type=4) AS abnormal_type_4_cnt,
                        sumIf(abnormal_cnt, abnormal_type=5) AS abnormal_type_5_cnt,
                        sumIf(abnormal_cnt, abnormal_type=6) AS abnormal_type_6_cnt,
                        sumIf(abnormal_cnt, abnormal_type=7) AS abnormal_type_7_cnt,
                        sumIf(abnormal_cnt, abnormal_type=8) AS abnormal_type_8_cnt,
                        sumIf(abnormal_cnt, abnormal_type=9) AS abnormal_type_9_cnt,
                        sumIf(abnormal_cnt, abnormal_type=10) AS abnormal_type_10_cnt,
                        sumIf(abnormal_cnt, abnormal_type=11) AS abnormal_type_11_cnt,
                        sumIf(abnormal_cnt, abnormal_type=12) AS abnormal_type_12_cnt,
                        sumIf(abnormal_cnt, abnormal_type=13) AS abnormal_type_13_cnt,
                        sumIf(abnormal_cnt, abnormal_type=14) AS abnormal_type_14_cnt,
                        sumIf(abnormal_cnt, abnormal_type=15) AS abnormal_type_15_cnt,
                        sumIf(abnormal_cnt, abnormal_type=16) AS abnormal_type_16_cnt,
                        sumIf(abnormal_cnt, abnormal_type=17) AS abnormal_type_17_cnt,
                        sumIf(abnormal_cnt, abnormal_type=18) AS abnormal_type_18_cnt,
                        sumIf(abnormal_cnt, abnormal_type=19) AS abnormal_type_19_cnt,
                        sumIf(abnormal_cnt, abnormal_type=20) AS abnormal_type_20_cnt,
                        sumIf(abnormal_cnt, abnormal_type=21) AS abnormal_type_21_cnt,
                        sumIf(abnormal_cnt, abnormal_type=22) AS abnormal_type_22_cnt,
                        sumIf(abnormal_cnt, abnormal_type=23) AS abnormal_type_23_cnt,
                        sumIf(abnormal_cnt, abnormal_type=24) AS abnormal_type_24_cnt,
                        sumIf(abnormal_cnt, abnormal_type=25) AS abnormal_type_25_cnt,
                        sumIf(abnormal_cnt, abnormal_type=26) AS abnormal_type_26_cnt,
                        sumIf(abnormal_cnt, abnormal_type=27) AS abnormal_type_27_cnt,
                        sumIf(abnormal_cnt, abnormal_type=28) AS abnormal_type_28_cnt,
                        sumIf(abnormal_cnt, abnormal_type=29) AS abnormal_type_29_cnt
                    FROM dwd.xdqc_dialog_all
                    ARRAY JOIN
                        abnormals_type AS abnormal_type, 
                        abnormals_count AS abnormal_cnt
                    WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
                    AND snick GLOBAL IN (
                        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                        -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                        SELECT distinct snick
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = toYYYYMMDD(yesterday())
                        AND platform = 'tb'
                        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                    )
                    AND abnormal_cnt!=0
                    GROUP BY platform, seller_nick, snick
                ) AS ai_abnormal_info
                GLOBAL FULL OUTER JOIN (
                    -- AI质检-子账号维度加分质检项触发次数统计
                    SELECT
                        platform,
                        seller_nick,
                        snick,
                        sumIf(excellent_cnt, excellent_type=1) AS excellent_type_1_cnt,
                        sumIf(excellent_cnt, excellent_type=2) AS excellent_type_2_cnt,
                        sumIf(excellent_cnt, excellent_type=3) AS excellent_type_3_cnt,
                        sumIf(excellent_cnt, excellent_type=4) AS excellent_type_4_cnt,
                        sumIf(excellent_cnt, excellent_type=5) AS excellent_type_5_cnt,
                        sumIf(excellent_cnt, excellent_type=6) AS excellent_type_6_cnt,
                        sumIf(excellent_cnt, excellent_type=7) AS excellent_type_7_cnt,
                        sumIf(excellent_cnt, excellent_type=8) AS excellent_type_8_cnt,
                        sumIf(excellent_cnt, excellent_type=9) AS excellent_type_9_cnt,
                        sumIf(excellent_cnt, excellent_type=10) AS excellent_type_10_cnt,
                        sumIf(excellent_cnt, excellent_type=11) AS excellent_type_11_cnt,
                        sumIf(excellent_cnt, excellent_type=12) AS excellent_type_12_cnt,
                        sumIf(excellent_cnt, excellent_type=13) AS excellent_type_13_cnt
                    FROM dwd.xdqc_dialog_all
                    ARRAY JOIN
                        excellents_type AS excellent_type, 
                        excellents_count AS excellent_cnt
                    WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
                    AND snick GLOBAL IN (
                        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                        -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                        SELECT distinct snick
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = toYYYYMMDD(yesterday())
                        AND platform = 'tb'
                        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                    )
                    AND excellent_cnt!=0
                    GROUP BY platform, seller_nick, snick
                ) AS ai_excellent_info
                USING(platform, seller_nick, snick)
            ) AS ai_abnormal_excellent_info
            GLOBAL FULL OUTER JOIN (
                -- AI质检-子账号维度情绪质检结果
                SELECT *
                FROM (
                    -- AI质检-子账号维度顾客情绪质检项触发次数统计
                    SELECT
                        platform,
                        seller_nick,
                        snick,
                        sumIf(c_emotion_count,c_emotion_type=1) AS c_emotion_type_1_cnt,
                        sumIf(c_emotion_count,c_emotion_type=2) AS c_emotion_type_2_cnt,
                        sumIf(c_emotion_count,c_emotion_type=3) AS c_emotion_type_3_cnt,
                        sumIf(c_emotion_count,c_emotion_type=4) AS c_emotion_type_4_cnt,
                        sumIf(c_emotion_count,c_emotion_type=5) AS c_emotion_type_5_cnt,
                        sumIf(c_emotion_count,c_emotion_type=6) AS c_emotion_type_6_cnt,
                        sumIf(c_emotion_count,c_emotion_type=7) AS c_emotion_type_7_cnt,
                        sumIf(c_emotion_count,c_emotion_type=8) AS c_emotion_type_8_cnt,
                        sumIf(c_emotion_count,c_emotion_type=9) AS c_emotion_type_9_cnt
                    FROM dwd.xdqc_dialog_all
                    ARRAY JOIN
                        c_emotion_type,
                        c_emotion_count
                    WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
                    AND platform = 'tb'
                    AND snick GLOBAL IN (
                        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                        -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                        SELECT distinct snick
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = toYYYYMMDD(yesterday())
                        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                        AND platform = 'tb'
                    )
                    AND c_emotion_count!=0
                    GROUP BY platform, seller_nick, snick
                ) AS ai_c_emotion_info
                GLOBAL FULL OUTER JOIN(
                    -- AI质检-子账号维度客服情绪质检项触发次数统计
                    SELECT
                        platform,
                        seller_nick,
                        snick,
                        sumIf(s_emotion_count, s_emotion_type=8) AS s_emotion_type_8_cnt
                    FROM dwd.xdqc_dialog_all
                    ARRAY JOIN
                        s_emotion_type,
                        s_emotion_count
                    WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
                    AND platform = 'tb'
                    AND snick GLOBAL IN (
                        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                        -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                        SELECT distinct snick
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = toYYYYMMDD(yesterday())
                        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                        AND platform = 'tb'
                    )
                    AND s_emotion_count!=0
                    GROUP BY platform, seller_nick, snick
                ) AS ai_s_emotion_info
                USING(platform, seller_nick, snick)
            ) AS ai_emotion_info
            USING(platform, seller_nick, snick)
        ) AS ai_check_info
        USING(platform, seller_nick, snick)
    ) AS snick_dialog_stat_ai_check_info
    GROUP BY platform
) AS dialog_stat_ai_check_info

GLOBAL FULL OUTER JOIN (
    -- 人工+自定义质检结果
    SELECT *
    FROM (
        -- 人工质检结果
        SELECT
            platform,
            groupArray(tag_name) AS human_check_tag_name_arr,
            groupArray(tag_cnt) AS human_check_tag_cnt_arr
        FROM (
            -- 人工质检-平台维度人工质检扣分标签次数统计
            SELECT
                platform,
                tag_score_stat_id AS tag_id,
                sum(tag_score_stat_count + tag_score_stat_md) AS tag_cnt
            FROM (
                -- 针对字段缺失的历史数据进行转换, 使其数据为0, 保证语法正确
                SELECT
                    platform,
                    seller_nick,
                    snick,
                    tag_score_stats_id AS tag_score_stats_id,
                    -- 缺失历史数据直接为0, 对齐数组长度
                    if(
                        length(tag_score_stats_count)!=length(tag_score_stats_id),
                        arrayResize([0],length(tag_score_stats_id),0),
                        tag_score_stats_count
                    ) AS tag_score_stats_count,
                    if(
                        length(tag_score_stats_md)!=length(tag_score_stats_id),
                        arrayResize([0],length(tag_score_stats_id),0),
                        tag_score_stats_md
                    ) AS tag_score_stats_md
                FROM dwd.xdqc_dialog_all
                WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
                AND snick GLOBAL IN (
                    -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                    -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                    SELECT distinct snick
                    FROM ods.xinghuan_employee_snick_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND platform = 'tb'
                    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                    -- 下拉框-子账号分组
                    AND (
                        '{{ department_ids }}'=''
                        OR
                        department_id IN splitByChar(',','{{ department_ids }}')
                    )
                )
                -- 清除没有打标的数据, 减小计算量
                AND tag_score_stats_id!=[]
            ) AS dialog_info
            ARRAY JOIN
                tag_score_stats_id AS tag_score_stat_id,
                tag_score_stats_count AS tag_score_stat_count,
                tag_score_stats_md AS tag_score_stat_md
            -- 清除次数为0的历史缺失数据, 减小计算量
            WHERE (tag_score_stat_count + tag_score_stat_md)!=0
            GROUP BY platform, tag_score_stat_id
            UNION ALL
            -- 人工质检-平台维度人工质检加分标签次数统计
            SELECT
                platform,
                tag_score_add_stat_id AS tag_id,
                sum(tag_score_add_stat_count + tag_score_add_stat_md) AS tag_cnt
            FROM (
                    -- 针对字段缺失的历史数据进行转换, 使其数据为0, 保证语法正确
                    SELECT
                        platform,
                        seller_nick,
                        snick,
                        tag_score_add_stats_id AS tag_score_add_stats_id,
                        -- 缺失历史数据直接为0, 对齐数组长度
                        if(
                            length(tag_score_add_stats_count)!=length(tag_score_add_stats_id),
                            arrayResize([0],length(tag_score_add_stats_id),0),
                            tag_score_add_stats_count
                        ) AS tag_score_add_stats_count,
                        if(
                            length(tag_score_add_stats_md)!=length(tag_score_add_stats_id),
                            arrayResize([0],length(tag_score_add_stats_id),0),
                            tag_score_add_stats_md
                        ) AS tag_score_add_stats_md
                    FROM dwd.xdqc_dialog_all
                    WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
                    AND snick GLOBAL IN (
                        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                        -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                        SELECT distinct snick
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = toYYYYMMDD(yesterday())
                        AND platform = 'tb'
                        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                        -- 下拉框-子账号分组
                        AND (
                            '{{ department_ids }}'=''
                            OR
                            department_id IN splitByChar(',','{{ department_ids }}')
                        )
                    )
                    -- 清除没有打标的数据, 减小计算量
                    AND tag_score_add_stats_id!=[]
            ) AS dialog_info
            ARRAY JOIN
                    tag_score_add_stats_id AS tag_score_add_stat_id,
                    tag_score_add_stats_count AS tag_score_add_stat_count,
                    tag_score_add_stats_md AS tag_score_add_stat_md
            -- 清除次数为0的历史缺失数据, 减小计算量
            WHERE (tag_score_add_stat_count + tag_score_add_stat_md)!=0
            GROUP BY platform, tag_score_add_stat_id

        ) AS platform_human_check_stat
        GLOBAL LEFT JOIN (
            SELECT
                _id AS tag_id,
                name AS tag_name
            FROM ods.xdqc_tag_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        ) AS human_check_tag_info
        USING(tag_id)
        GROUP BY platform
    ) AS human_check_info
    GLOBAL FULL OUTER JOIN (
        -- 自定义质检结果
        SELECT
            platform,
            groupArray(tag_name) AS customize_check_tag_name_arr,
            groupArray(tag_cnt) AS customize_check_tag_cnt_arr
        FROM (
            -- 自定义质检-平台维度扣分质检项触发次数统计
            SELECT
                platform,
                rule_stats_tag_id AS tag_id,
                sum(rule_stats_tag_count) AS tag_cnt
            FROM dwd.xdqc_dialog_all
            ARRAY JOIN
                rule_stats_id AS rule_stats_tag_id,
                rule_stats_count AS rule_stats_tag_count
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
            AND snick GLOBAL IN (
                -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                SELECT distinct snick
                FROM ods.xinghuan_employee_snick_all
                WHERE day = toYYYYMMDD(yesterday())
                AND platform = 'tb'
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                -- 下拉框-子账号分组
                AND (
                    '{{ department_ids }}'=''
                    OR
                    department_id IN splitByChar(',','{{ department_ids }}')
                )
            )
            -- 清除没有打标的数据, 减小计算量
            AND rule_stats_id!=[]
            GROUP BY platform, rule_stats_tag_id

            UNION ALL

            -- 自定义质检-平台维度加分质检项触发次数统计
            SELECT
                platform,
                rule_add_stats_tag_id AS tag_id,
                sum(rule_add_stats_tag_count) AS tag_cnt
            FROM dwd.xdqc_dialog_all
            ARRAY JOIN
                rule_add_stats_id AS rule_add_stats_tag_id,
                rule_add_stats_count AS rule_add_stats_tag_count
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
            AND snick GLOBAL IN (
                -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                SELECT distinct snick
                FROM ods.xinghuan_employee_snick_all
                WHERE day = toYYYYMMDD(yesterday())
                AND platform = 'tb'
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                -- 下拉框-子账号分组
                AND (
                    '{{ department_ids }}'=''
                    OR
                    department_id IN splitByChar(',','{{ department_ids }}')
                )
            )
            -- 清除没有打标的数据, 减小计算量
            AND rule_add_stats_id!=[]
            GROUP BY platform, rule_add_stats_tag_id
        ) AS customize_check_stat
        GLOBAL LEFT JOIN (
            SELECT
                _id AS tag_id,
                name AS tag_name
            FROM ods.xinghuan_customize_rule_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        ) AS customize_tag_info
        USING(tag_id)
        GROUP BY platform
    ) AS customize_check_info
    USING(platform)
) AS human_customize_check_info
USING(platform)


