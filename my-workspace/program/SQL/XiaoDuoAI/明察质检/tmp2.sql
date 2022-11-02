-- 写入数据
-- 京东, 5月份数据
ALTER TABLE tmp.xqc_qc_report_snick_local ON CLUSTER cluster_3s_2r
DELETE WHERE day BETWEEN 20220711 AND 20220720 SETTINGS mutations_sync = 2, replication_alter_partitions_sync = 2

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
            WHERE toYYYYMMDD(begin_time) BETWEEN 20220711 AND 20220720
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
            SELECT
                day, platform, seller_nick, snick,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=1) AS abnormal_type_1_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=2) AS abnormal_type_2_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=3) AS abnormal_type_3_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=4) AS abnormal_type_4_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=5) AS abnormal_type_5_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=6) AS abnormal_type_6_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=7) AS abnormal_type_7_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=8) AS abnormal_type_8_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=9) AS abnormal_type_9_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=10) AS abnormal_type_10_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=11) AS abnormal_type_11_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=12) AS abnormal_type_12_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=13) AS abnormal_type_13_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=14) AS abnormal_type_14_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=15) AS abnormal_type_15_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=16) AS abnormal_type_16_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=17) AS abnormal_type_17_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=18) AS abnormal_type_18_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=19) AS abnormal_type_19_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=20) AS abnormal_type_20_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=21) AS abnormal_type_21_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=22) AS abnormal_type_22_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=23) AS abnormal_type_23_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=24) AS abnormal_type_24_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=25) AS abnormal_type_25_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=26) AS abnormal_type_26_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=27) AS abnormal_type_27_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=28) AS abnormal_type_28_cnt,
                sumIf(tag_cnt, tag_type='ai_abnormal' AND tag_num=29) AS abnormal_type_29_cnt,
                sumIf(tag_cnt, tag_type='ai_excellent' AND tag_num=1) AS excellent_type_1_cnt,
                sumIf(tag_cnt, tag_type='ai_excellent' AND tag_num=2) AS excellent_type_2_cnt,
                sumIf(tag_cnt, tag_type='ai_excellent' AND tag_num=3) AS excellent_type_3_cnt,
                sumIf(tag_cnt, tag_type='ai_excellent' AND tag_num=4) AS excellent_type_4_cnt,
                sumIf(tag_cnt, tag_type='ai_excellent' AND tag_num=5) AS excellent_type_5_cnt,
                sumIf(tag_cnt, tag_type='ai_excellent' AND tag_num=6) AS excellent_type_6_cnt,
                sumIf(tag_cnt, tag_type='ai_excellent' AND tag_num=7) AS excellent_type_7_cnt,
                sumIf(tag_cnt, tag_type='ai_excellent' AND tag_num=8) AS excellent_type_8_cnt,
                sumIf(tag_cnt, tag_type='ai_excellent' AND tag_num=9) AS excellent_type_9_cnt,
                sumIf(tag_cnt, tag_type='ai_excellent' AND tag_num=10) AS excellent_type_10_cnt,
                sumIf(tag_cnt, tag_type='ai_excellent' AND tag_num=11) AS excellent_type_11_cnt,
                sumIf(tag_cnt, tag_type='ai_excellent' AND tag_num=12) AS excellent_type_12_cnt,
                sumIf(tag_cnt, tag_type='ai_excellent' AND tag_num=13) AS excellent_type_13_cnt,
                sumIf(tag_cnt, tag_type='ai_c_emotion' AND tag_num=1) AS c_emotion_type_1_cnt,
                sumIf(tag_cnt, tag_type='ai_c_emotion' AND tag_num=2) AS c_emotion_type_2_cnt,
                sumIf(tag_cnt, tag_type='ai_c_emotion' AND tag_num=3) AS c_emotion_type_3_cnt,
                sumIf(tag_cnt, tag_type='ai_c_emotion' AND tag_num=4) AS c_emotion_type_4_cnt,
                sumIf(tag_cnt, tag_type='ai_c_emotion' AND tag_num=5) AS c_emotion_type_5_cnt,
                sumIf(tag_cnt, tag_type='ai_c_emotion' AND tag_num=6) AS c_emotion_type_6_cnt,
                sumIf(tag_cnt, tag_type='ai_c_emotion' AND tag_num=7) AS c_emotion_type_7_cnt,
                sumIf(tag_cnt, tag_type='ai_c_emotion' AND tag_num=8) AS c_emotion_type_8_cnt,
                sumIf(tag_cnt, tag_type='ai_c_emotion' AND tag_num=9) AS c_emotion_type_9_cnt,
                sumIf(tag_cnt, tag_type='ai_s_emotion' AND tag_num=8) AS s_emotion_type_8_cnt
            FROM (
                SELECT
                    toYYYYMMDD(begin_time) AS day,
                    platform,
                    seller_nick,
                    snick,
                    -- AI质检-质检项类型
                    arrayConcat(
                        -- AI质检-扣分项
                        arrayResize(['ai_abnormal'], length(abnormals_type), 'ai_abnormal'),
                        -- AI质检-加分项
                        arrayResize(['ai_excellent'], length(excellents_type), 'ai_excellent'),
                        -- AI质检-买家情绪项
                        arrayResize(['ai_c_emotion'], length(c_emotion_type), 'ai_c_emotion'),
                        -- AI质检-客服情绪项
                        arrayResize(['ai_s_emotion'], length(s_emotion_type), 'ai_s_emotion')
                    ) AS tag_types,
                    -- AI质检-质检项ID
                    arrayConcat(
                        abnormals_type, excellents_type, c_emotion_type, s_emotion_type
                    ) AS tag_nums,
                    -- AI质检-质检项次数
                    arrayConcat(
                        abnormals_count, excellents_count, c_emotion_count, s_emotion_count
                    ) AS tag_cnts
                FROM dwd.xdqc_dialog_all
                WHERE toYYYYMMDD(begin_time) BETWEEN 20220711 AND 20220720
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
                -- 顾家定制化需求新增条件
                -- 最近订单是未创建/已下单/已付定金的会话
                AND order_info_status[1] IN ('','created','deposited')
                -- 排除无效会话(买家必须有发送消息)
                AND (question_count!=0)
            ) AS ai_check
            ARRAY JOIN
                tag_types AS tag_type,
                tag_nums AS tag_num,
                tag_cnts AS tag_cnt
            WHERE tag_cnt!=0
            GROUP BY day, platform, seller_nick, snick
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
            groupArray(tag_cnt_sum) AS human_check_tag_cnt_arr
        FROM (
            -- 人工质检-子账号维度扣分标签触发次数统计
            SELECT
                day,
                platform,
                seller_nick,
                snick,
                tag_id,
                sum(tag_cnt + tag_md) AS tag_cnt_sum
            FROM (
                -- 针对字段缺失的历史数据进行转换, 使其数据为0, 保证语法正确
                SELECT
                    toYYYYMMDD(begin_time) AS day,
                    platform,
                    seller_nick,
                    snick,
                    -- 人工质检-标签ID
                    arrayConcat(
                        -- 人工质检-扣分标签
                        tag_score_stats_id,
                        -- 人工质检-扣分标签
                        tag_score_add_stats_id
                    ) AS tag_ids,
                    -- 人工质检-打标次数
                    arrayConcat(
                        -- 人工质检-扣分标签
                        if(
                            length(tag_score_stats_count)!=length(tag_score_stats_id),
                            arrayResize([0],length(tag_score_stats_id),0),
                            tag_score_stats_count
                        ),
                        -- 人工质检-加分标签
                        if(
                            length(tag_score_add_stats_count)!=length(tag_score_add_stats_id),
                            arrayResize([0],length(tag_score_add_stats_id),0),
                            tag_score_add_stats_count
                        )
                    ) AS tag_cnts,
                    -- 人工质检-会话打标标识
                    arrayConcat(
                        -- 人工质检-扣分标签会话打标标识
                        if(
                            length(tag_score_stats_md)!=length(tag_score_stats_id),
                            arrayResize([0],length(tag_score_stats_id),0),
                            tag_score_stats_md
                        ),
                        -- 人工质检-加分标签会话打标标识
                        if(
                            length(tag_score_add_stats_md)!=length(tag_score_add_stats_id),
                            arrayResize([0],length(tag_score_add_stats_id),0),
                            tag_score_add_stats_md
                        )
                    ) AS tag_mds
                FROM dwd.xdqc_dialog_all
                WHERE toYYYYMMDD(begin_time) BETWEEN 20220711 AND 20220720
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
                -- 顾家定制化需求新增条件
                -- 最近订单是未创建/已下单/已付定金的会话
                AND order_info_status[1] IN ('','created','deposited')
                -- 排除无效会话(买家必须有发送消息)
                AND (question_count!=0)
            ) AS transformed_dialog_info
            ARRAY JOIN
                tag_ids AS tag_id,
                tag_cnts AS tag_cnt,
                tag_mds AS tag_md
            GROUP BY day, platform, seller_nick, snick, tag_id
            -- 清除空数据
            HAVING tag_cnt_sum != 0
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
        groupArray(tag_sum) AS customize_check_tag_cnt_arr
    FROM (
        -- 自定义质检-平台维度质检项触发次数统计
        SELECT
            toYYYYMMDD(begin_time) AS day,
            platform,
            seller_nick,
            snick,
            tag_id,
            sum(tag_cnt) AS tag_sum
        FROM dwd.xdqc_dialog_all
        ARRAY JOIN
            arrayConcat(
                -- 自定义质检-扣分质检项
                rule_stats_id,
                -- 自定义质检-加分质检项
                rule_add_stats_id,
                -- 自定义质检-会话质检项
                top_xrules_id,
                -- 自定义质检-消息质检项
                xrule_stats_id
            ) AS tag_id,
            arrayConcat(
                -- 自定义质检-扣分质检项触发次数
                rule_stats_count,
                -- 自定义质检-加分质检项触发次数统计
                rule_add_stats_count,
                -- 自定义质检-会话质检项触发次数统计
                top_xrules_count,
                -- 自定义质检-消息质检项触发次数统计
                xrule_stats_count
            ) AS tag_cnt
        WHERE toYYYYMMDD(begin_time) BETWEEN 20220711 AND 20220720
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
        -- 顾家定制化需求新增条件
        -- 最近订单是未创建/已下单/已付定金的会话
        AND order_info_status[1] IN ('','created','deposited')
        -- 排除无效会话(买家必须有发送消息)
        AND (question_count!=0)
        GROUP BY day, platform, seller_nick, snick, tag_id
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
    HAVING tag_sum!=0
) AS customize_check_info
USING(day, platform, seller_nick, snick)