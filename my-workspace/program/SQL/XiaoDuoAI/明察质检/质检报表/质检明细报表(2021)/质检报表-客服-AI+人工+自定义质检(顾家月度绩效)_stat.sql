-- 写入数据
-- 京东, 5月份数据
ALTER TABLE tmp.xqc_qc_report_snick_local ON CLUSTER cluster_3s_2r
DELETE WHERE day BETWEEN 20220701 AND 20220710 SETTINGS mutations_sync = 2, replication_alter_partitions_sync = 2

INSERT INTO tmp.xqc_qc_report_snick_all
-- 质检结果总览+AI质检结果+人工质检结果+自定义质检结果-子账号维度
SELECT *
FROM (
    -- 质检结果总览+AI质检结果+人工质检结果-子账号维度
    SELECT *
    FROM (
        -- 质检结果总览+AI质检结果-子账号维度
        SELECT *
        FROM (
            -- 质检结果总览-子账号维度
            SELECT
                toYYYYMMDD(begin_time) AS day,
                platform,
                seller_nick,
                snick,
                COUNT(1) AS dialog_cnt,
                sum(score) AS score,
                sum(score_add) AS score_add,
                sum(mark_score) AS mark_score,
                sum(mark_score_add) AS mark_score_add,
                sum(
                    arraySum(rule_stats_score)
                    +
                    negate(arraySum(arrayFilter(x->x<0, xrule_stats_score)))
                    +
                    negate(arraySum(arrayFilter(x->x<0, top_xrules_score)))
                ) AS rule_score,
                sum(
                    arraySum(rule_add_stats_score)
                    +
                    arraySum(arrayFilter(x->x>0, xrule_stats_score))
                    +
                    arraySum(arrayFilter(x->x>0, top_xrules_score))
                ) AS rule_score_add,
                score - mark_score - rule_score AS ai_score,
                score_add - mark_score_add - rule_score_add AS ai_score_add,
                sum(arraySum(abnormals_count)!=0) AS abnormal_dialog_cnt,
                sum(arraySum(excellents_count)!=0) AS excellents_dialog_cnt,
                sum(length(mark_ids)!=0) AS mark_dialog_cnt,
                sum(length(tag_score_stats_id)!=0) AS tag_score_dialog_cnt,
                sum(length(tag_score_add_stats_id)!=0) AS tag_score_add_dialog_cnt,
                sum((
                        length(rule_stats_id)
                        +
                        length(arrayFilter(x->x<0, xrule_stats_score))
                        +
                        length(arrayFilter(x->x<0, top_xrules_score))
                    )!=0
                ) AS rule_dialog_cnt,
                sum((
                        length(rule_add_stats_id)
                        +
                        length(arrayFilter(x->x>0, xrule_stats_score))
                        +
                        length(arrayFilter(x->x>0, top_xrules_score))
                    )!=0
                ) AS rule_add_dialog_cnt
            FROM dwd.xdqc_dialog_all
            WHERE toYYYYMMDD(begin_time) BETWEEN 20220701 AND 20220710
            AND platform = 'jd'
            AND seller_nick GLOBAL IN (
                -- 查询对应企业-平台的店铺
                SELECT DISTINCT seller_nick
                FROM xqc_dim.xqc_shop_all
                WHERE day=toYYYYMMDD(yesterday())
                AND platform = 'jd'
                AND company_id = '614d86d84eed94e6fc980b1c'
            )
            AND snick GLOBAL IN (
                -- 获取最新版本的维度数据(T+1)
                SELECT distinct snick
                FROM ods.xinghuan_employee_snick_all
                WHERE day = toYYYYMMDD(yesterday())
                AND platform = 'jd'
                AND company_id = '614d86d84eed94e6fc980b1c'
            )
            -- 顾家定制化需求新增条件
            -- 最近订单是未创建/已下单/已付定金的会话
            AND order_info_status[1] IN ('','created','deposited')
            -- 排除无效会话(买家必须有发送消息)
            AND (question_count!=0)
            GROUP BY day, platform, seller_nick, snick
        ) AS stat_info
        GLOBAL FULL OUTER JOIN (
            -- AI质检结果-子账号维度
            SELECT *
            FROM (
                -- AI质检-子账号维度加分扣分质检项触发次数统计
                SELECT *
                FROM (
                    -- AI质检-子账号维度扣分质检项触发次数统计
                    SELECT
                        toYYYYMMDD(begin_time) AS day,
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
                    WHERE toYYYYMMDD(begin_time) BETWEEN 20220701 AND 20220710
                    AND platform = 'jd'
                    AND seller_nick GLOBAL IN (
                        -- 查询对应企业-平台的店铺
                        SELECT DISTINCT seller_nick
                        FROM xqc_dim.xqc_shop_all
                        WHERE day=toYYYYMMDD(yesterday())
                        AND platform = 'jd'
                        AND company_id = '614d86d84eed94e6fc980b1c'
                    )
                    AND snick GLOBAL IN (
                        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                        -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                        SELECT distinct snick
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = toYYYYMMDD(yesterday())
                        AND platform = 'jd'
                        AND company_id = '614d86d84eed94e6fc980b1c'
                    )
                    AND abnormal_cnt!=0
                    -- 顾家定制化需求新增条件
                    -- 最近订单是未创建/已下单/已付定金的会话
                    AND order_info_status[1] IN ('','created','deposited')
                    -- 排除无效会话(买家必须有发送消息)
                    AND (question_count!=0)
                    GROUP BY day, platform, seller_nick, snick
                ) AS ai_abnormal_info
                GLOBAL FULL OUTER JOIN (
                    -- AI质检-子账号维度加分质检项触发次数统计
                    SELECT
                        toYYYYMMDD(begin_time) AS day,
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
                    WHERE toYYYYMMDD(begin_time) BETWEEN 20220701 AND 20220710
                    AND platform = 'jd'
                    AND seller_nick GLOBAL IN (
                        -- 查询对应企业-平台的店铺
                        SELECT DISTINCT seller_nick
                        FROM xqc_dim.xqc_shop_all
                        WHERE day=toYYYYMMDD(yesterday())
                        AND platform = 'jd'
                        AND company_id = '614d86d84eed94e6fc980b1c'
                    )
                    AND snick GLOBAL IN (
                        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                        -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                        SELECT distinct snick
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = toYYYYMMDD(yesterday())
                        AND platform = 'jd'
                        AND company_id = '614d86d84eed94e6fc980b1c'
                    )
                    AND excellent_cnt!=0
                    -- 顾家定制化需求新增条件
                    -- 最近订单是未创建/已下单/已付定金的会话
                    AND order_info_status[1] IN ('','created','deposited')
                    -- 排除无效会话(买家必须有发送消息)
                    AND (question_count!=0)
                    GROUP BY day, platform, seller_nick, snick
                ) AS ai_excellent_info
                USING(day, platform, seller_nick, snick)
            ) AS ai_abnormal_excellent_info
            GLOBAL FULL OUTER JOIN (
                -- AI质检-子账号维度情绪质检项触发次数统计
                SELECT *
                FROM (
                    -- AI质检-子账号维度顾客情绪质检项触发次数统计
                    SELECT
                        toYYYYMMDD(begin_time) AS day,
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
                    WHERE toYYYYMMDD(begin_time) BETWEEN 20220701 AND 20220710
                    AND platform = 'jd'
                    AND seller_nick GLOBAL IN (
                        -- 查询对应企业-平台的店铺
                        SELECT DISTINCT seller_nick
                        FROM xqc_dim.xqc_shop_all
                        WHERE day=toYYYYMMDD(yesterday())
                        AND platform = 'jd'
                        AND company_id = '614d86d84eed94e6fc980b1c'
                    )
                    AND snick GLOBAL IN (
                        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                        -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                        SELECT distinct snick
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = toYYYYMMDD(yesterday())
                        AND company_id = '614d86d84eed94e6fc980b1c'
                        AND platform = 'jd'
                    )
                    AND c_emotion_count!=0
                    -- 顾家定制化需求新增条件
                    -- 最近订单是未创建/已下单/已付定金的会话
                    AND order_info_status[1] IN ('','created','deposited')
                    -- 排除无效会话(买家必须有发送消息)
                    AND (question_count!=0)
                    GROUP BY day, platform, seller_nick, snick
                ) AS ai_c_emotion_info
                GLOBAL FULL OUTER JOIN(
                    -- AI质检-子账号维度客服情绪质检项触发次数统计
                    SELECT
                        toYYYYMMDD(begin_time) AS day,
                        platform,
                        seller_nick,
                        snick,
                        sumIf(s_emotion_count, s_emotion_type=8) AS s_emotion_type_8_cnt
                    FROM dwd.xdqc_dialog_all
                    ARRAY JOIN
                        s_emotion_type,
                        s_emotion_count
                    WHERE toYYYYMMDD(begin_time) BETWEEN 20220701 AND 20220710
                    AND platform = 'jd'
                    AND seller_nick GLOBAL IN (
                        -- 查询对应企业-平台的店铺
                        SELECT DISTINCT seller_nick
                        FROM xqc_dim.xqc_shop_all
                        WHERE day=toYYYYMMDD(yesterday())
                        AND platform = 'jd'
                        AND company_id = '614d86d84eed94e6fc980b1c'
                    )
                    AND snick GLOBAL IN (
                        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                        -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                        SELECT distinct snick
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = toYYYYMMDD(yesterday())
                        AND company_id = '614d86d84eed94e6fc980b1c'
                        AND platform = 'jd'
                    )
                    AND s_emotion_count!=0
                    -- 顾家定制化需求新增条件
                    -- 最近订单是未创建/已下单/已付定金的会话
                    AND order_info_status[1] IN ('','created','deposited')
                    -- 排除无效会话(买家必须有发送消息)
                    AND (question_count!=0)
                    
                    GROUP BY day, platform, seller_nick, snick
                ) AS ai_s_emotion_info
                USING(day, platform, seller_nick, snick)
            ) AS ai_emotion_info
            USING(day, platform, seller_nick, snick)
        ) AS ai_check_info
        USING(day, platform, seller_nick, snick)
    ) AS stat_ai_check_info
    GLOBAL FULL OUTER JOIN (
        -- 人工质检结果-子账号维度
        SELECT
            day,
            platform,
            seller_nick,
            snick,
            groupArray(tag_name) AS human_check_tag_name_arr,
            groupArray(tag_cnt) AS human_check_tag_cnt_arr
        FROM (
            -- 人工质检-子账号维度扣分标签触发次数统计
            SELECT
                day,
                platform,
                seller_nick,
                snick,
                tag_id,
                sum(tag_score_stat_count + tag_score_stat_md) AS tag_cnt
            FROM (
                -- 针对字段缺失的历史数据进行转换, 使其数据为0, 保证语法正确
                SELECT
                    toYYYYMMDD(begin_time) AS day,
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
                WHERE toYYYYMMDD(begin_time) BETWEEN 20220701 AND 20220710
                AND platform = 'jd'
                AND seller_nick GLOBAL IN (
                    -- 查询对应企业-平台的店铺
                    SELECT DISTINCT seller_nick
                    FROM xqc_dim.xqc_shop_all
                    WHERE day=toYYYYMMDD(yesterday())
                    AND platform = 'jd'
                    AND company_id = '614d86d84eed94e6fc980b1c'
                )
                AND snick GLOBAL IN (
                    -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                    -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                    SELECT distinct snick
                    FROM ods.xinghuan_employee_snick_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND platform = 'jd'
                    AND company_id = '614d86d84eed94e6fc980b1c'
                )
                -- 清除没有打标的数据, 减小计算量
                AND tag_score_stats_id!=[]
                -- 顾家定制化需求新增条件
                -- 最近订单是未创建/已下单/已付定金的会话
                AND order_info_status[1] IN ('','created','deposited')
                -- 排除无效会话(买家必须有发送消息)
                AND (question_count!=0)
            ) AS transformed_dialog_info
            ARRAY JOIN
                tag_score_stats_id AS tag_id,
                tag_score_stats_count AS tag_score_stat_count,
                tag_score_stats_md AS tag_score_stat_md
            -- 清除空数据
            WHERE tag_score_stats_id!=[]
            GROUP BY day, platform, seller_nick, snick, tag_id
            
            UNION ALL
            
            -- 人工质检-子账号维度加分标签触发次数统计
            SELECT
                day,
                platform,
                seller_nick,
                snick,
                tag_id,
                sum(tag_score_add_stat_count + tag_score_add_stat_md) AS tag_cnt
            FROM (
                -- 针对字段缺失的历史数据进行转换, 使其数据为0, 保证语法正确
                SELECT
                    toYYYYMMDD(begin_time) AS day,
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
                WHERE toYYYYMMDD(begin_time) BETWEEN 20220701 AND 20220710
                AND platform = 'jd'
                AND seller_nick GLOBAL IN (
                    -- 查询对应企业-平台的店铺
                    SELECT DISTINCT seller_nick
                    FROM xqc_dim.xqc_shop_all
                    WHERE day=toYYYYMMDD(yesterday())
                    AND platform = 'jd'
                    AND company_id = '614d86d84eed94e6fc980b1c'
                )
                AND snick GLOBAL IN (
                    -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                    -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                    SELECT distinct snick
                    FROM ods.xinghuan_employee_snick_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND platform = 'jd'
                    AND company_id = '614d86d84eed94e6fc980b1c'
                )
                -- 清除没有打标的数据, 减小计算量
                AND tag_score_add_stats_id!=[]
                -- 顾家定制化需求新增条件
                -- 最近订单是未创建/已下单/已付定金的会话
                AND order_info_status[1] IN ('','created','deposited')
                -- 排除无效会话(买家必须有发送消息)
                AND (question_count!=0)
            ) AS transformed_dialog_info
            ARRAY JOIN
                tag_score_add_stats_id AS tag_id,
                tag_score_add_stats_count AS tag_score_add_stat_count,
                tag_score_add_stats_md AS tag_score_add_stat_md
            -- 清除空数据
            WHERE tag_score_add_stats_id!=[]
            GROUP BY day, platform, seller_nick, snick, tag_id
        ) AS human_check_tag_info
        GLOBAL LEFT JOIN (
            -- 获取人工质检项
            SELECT
                _id AS tag_id,
                name AS tag_name
            FROM xqc_dim.qc_rule_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'jd'
            AND company_id = '614d86d84eed94e6fc980b1c'
            AND rule_category = 2
        ) AS human_tag_info
        USING(tag_id)
        GROUP BY day, platform, seller_nick, snick
    ) AS human_check_info
    USING(day, platform, seller_nick, snick)
) AS stat_ai_human_check_info

GLOBAL FULL OUTER JOIN (
    -- 自定义质检结果-子账号维度
    SELECT
        day,
        platform,
        seller_nick,
        snick,
        groupArray(tag_name) AS customize_check_tag_name_arr,
        groupArray(tag_cnt) AS customize_check_tag_cnt_arr
    FROM (
        -- 自定义质检-平台维度扣分质检项触发次数统计
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            rule_stats_tag_id AS tag_id,
            sum(rule_stats_tag_count) AS tag_cnt
        FROM dwd.xdqc_dialog_all
        ARRAY JOIN
            rule_stats_id AS rule_stats_tag_id,
            rule_stats_count AS rule_stats_tag_count
        WHERE toYYYYMMDD(begin_time) BETWEEN 20220701 AND 20220710
        AND platform = 'jd'
        AND seller_nick GLOBAL IN (
            -- 查询对应企业-平台的店铺
            SELECT DISTINCT seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day=toYYYYMMDD(yesterday())
            AND platform = 'jd'
            AND company_id = '614d86d84eed94e6fc980b1c'
        )
        AND snick GLOBAL IN (
            -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
            -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'jd'
            AND company_id = '614d86d84eed94e6fc980b1c'
        )
        -- 清除没有打标的数据, 减小计算量
        AND rule_stats_id!=[]
        -- 顾家定制化需求新增条件
        -- 最近订单是未创建/已下单/已付定金的会话
        AND order_info_status[1] IN ('','created','deposited')
        -- 排除无效会话(买家必须有发送消息)
        AND (question_count!=0)
        GROUP BY day, platform, seller_nick, snick, rule_stats_tag_id

        UNION ALL
        -- 自定义质检-平台维度加分质检项触发次数统计
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            rule_add_stats_tag_id AS tag_id,
            sum(rule_add_stats_tag_count) AS tag_cnt
        FROM dwd.xdqc_dialog_all
        ARRAY JOIN
            rule_add_stats_id AS rule_add_stats_tag_id,
            rule_add_stats_count AS rule_add_stats_tag_count
        WHERE toYYYYMMDD(begin_time) BETWEEN 20220701 AND 20220710
        AND platform = 'jd'
        AND seller_nick GLOBAL IN (
            -- 查询对应企业-平台的店铺
            SELECT DISTINCT seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day=toYYYYMMDD(yesterday())
            AND platform = 'jd'
            AND company_id = '614d86d84eed94e6fc980b1c'
        )
        AND snick GLOBAL IN (
            -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
            -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'jd'
            AND company_id = '614d86d84eed94e6fc980b1c'
        )
        -- 清除没有打标的数据, 减小计算量
        AND rule_add_stats_id!=[]
        -- 顾家定制化需求新增条件
        -- 最近订单是未创建/已下单/已付定金的会话
        AND order_info_status[1] IN ('','created','deposited')
        -- 排除无效会话(买家必须有发送消息)
        AND (question_count!=0)
        GROUP BY day, platform, seller_nick, snick, rule_add_stats_tag_id

        UNION ALL
        -- 自定义质检-平台维度会话质检项触发次数统计
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            top_xrules_tag_id AS tag_id,
            sum(top_xrules_tag_count) AS tag_cnt
        FROM dwd.xdqc_dialog_all
        ARRAY JOIN
            top_xrules_id AS top_xrules_tag_id,
            top_xrules_count AS top_xrules_tag_count
        WHERE toYYYYMMDD(begin_time) BETWEEN 20220701 AND 20220710
        AND platform = 'jd'
        AND seller_nick GLOBAL IN (
            -- 查询对应企业-平台的店铺
            SELECT DISTINCT seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day=toYYYYMMDD(yesterday())
            AND platform = 'jd'
            AND company_id = '614d86d84eed94e6fc980b1c'
        )
        AND snick GLOBAL IN (
            -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
            -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'jd'
            AND company_id = '614d86d84eed94e6fc980b1c'
        )
        -- 清除没有打标的数据, 减小计算量
        AND top_xrules_id!=[]
        -- 顾家定制化需求新增条件
        -- 最近订单是未创建/已下单/已付定金的会话
        AND order_info_status[1] IN ('','created','deposited')
        -- 排除无效会话(买家必须有发送消息)
        AND (question_count!=0)
        GROUP BY day, platform, seller_nick, snick, top_xrules_tag_id

        UNION ALL
        -- 自定义质检-平台维度消息质检项触发次数统计
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            xrules_tag_id AS tag_id,
            sum(xrules_tag_count) AS tag_cnt
        FROM dwd.xdqc_dialog_all
        ARRAY JOIN
            xrule_stats_id AS xrules_tag_id,
            xrule_stats_count AS xrules_tag_count
        WHERE toYYYYMMDD(begin_time) BETWEEN 20220701 AND 20220710
        AND platform = 'jd'
        AND seller_nick GLOBAL IN (
            -- 查询对应企业-平台的店铺
            SELECT DISTINCT seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day=toYYYYMMDD(yesterday())
            AND platform = 'jd'
            AND company_id = '6234209693e6cbff31d6c118'
        )
        AND snick GLOBAL IN (
            -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
            -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'jd'
            AND company_id = '6234209693e6cbff31d6c118'
        )
        -- 清除没有打标的数据, 减小计算量
        AND xrule_stats_id!=[]
        -- 顾家定制化需求新增条件
        -- 最近订单是未创建/已下单/已付定金的会话
        AND order_info_status[1] IN ('','created','deposited')
        -- 排除无效会话(买家必须有发送消息)
        AND (question_count!=0)
        GROUP BY day, platform, seller_nick, snick, xrules_tag_id

    ) AS customize_check_stat
    GLOBAL LEFT JOIN (
        -- 获取自定义质检项
        SELECT
            _id AS tag_id,
            name AS tag_name
        FROM xqc_dim.qc_rule_all
        WHERE day = toYYYYMMDD(yesterday())
        AND platform = 'jd'
        AND company_id = '614d86d84eed94e6fc980b1c'
        AND rule_category = 3
    ) AS customize_tag_info
    USING(tag_id)
    GROUP BY day, platform, seller_nick, snick
) AS customize_check_info
USING(day, platform, seller_nick, snick)