-- 创建临时本地表
CREATE TABLE tmp.xqc_qc_report_snick_local ON CLUSTER cluster_3s_2r
(
    `day` Int64,
    `platform` String,
    `seller_nick` String,
    `snick` String,
    `dialog_cnt` UInt64,
    `score` Int64,
    `score_add` Int64,
    `mark_score` Int64,
    `mark_score_add` Int64,
    `rule_score` Int64,
    `rule_score_add` Int64,
    `ai_score` Int64,
    `ai_score_add` Int64,
    `abnormal_dialog_cnt` UInt64,
    `excellents_dialog_cnt` UInt64,
    `mark_dialog_cnt` UInt64,
    `tag_score_dialog_cnt` UInt64,
    `tag_score_add_dialog_cnt` UInt64,
    `rule_dialog_cnt` UInt64,
    `rule_add_dialog_cnt` UInt64,
    `abnormal_type_1_cnt` UInt64,
    `abnormal_type_2_cnt` UInt64,
    `abnormal_type_3_cnt` UInt64,
    `abnormal_type_4_cnt` UInt64,
    `abnormal_type_5_cnt` UInt64,
    `abnormal_type_6_cnt` UInt64,
    `abnormal_type_7_cnt` UInt64,
    `abnormal_type_8_cnt` UInt64,
    `abnormal_type_9_cnt` UInt64,
    `abnormal_type_10_cnt` UInt64,
    `abnormal_type_11_cnt` UInt64,
    `abnormal_type_12_cnt` UInt64,
    `abnormal_type_13_cnt` UInt64,
    `abnormal_type_14_cnt` UInt64,
    `abnormal_type_15_cnt` UInt64,
    `abnormal_type_16_cnt` UInt64,
    `abnormal_type_17_cnt` UInt64,
    `abnormal_type_18_cnt` UInt64,
    `abnormal_type_19_cnt` UInt64,
    `abnormal_type_20_cnt` UInt64,
    `abnormal_type_21_cnt` UInt64,
    `abnormal_type_22_cnt` UInt64,
    `abnormal_type_23_cnt` UInt64,
    `abnormal_type_24_cnt` UInt64,
    `abnormal_type_25_cnt` UInt64,
    `abnormal_type_26_cnt` UInt64,
    `abnormal_type_27_cnt` UInt64,
    `abnormal_type_28_cnt` UInt64,
    `abnormal_type_29_cnt` UInt64,
    `excellent_type_1_cnt` UInt64,
    `excellent_type_2_cnt` UInt64,
    `excellent_type_3_cnt` UInt64,
    `excellent_type_4_cnt` UInt64,
    `excellent_type_5_cnt` UInt64,
    `excellent_type_6_cnt` UInt64,
    `excellent_type_7_cnt` UInt64,
    `excellent_type_8_cnt` UInt64,
    `excellent_type_9_cnt` UInt64,
    `excellent_type_10_cnt` UInt64,
    `excellent_type_11_cnt` UInt64,
    `excellent_type_12_cnt` UInt64,
    `excellent_type_13_cnt` UInt64,
    `c_emotion_type_1_cnt` UInt64,
    `c_emotion_type_2_cnt` UInt64,
    `c_emotion_type_3_cnt` UInt64,
    `c_emotion_type_4_cnt` UInt64,
    `c_emotion_type_5_cnt` UInt64,
    `c_emotion_type_6_cnt` UInt64,
    `c_emotion_type_7_cnt` UInt64,
    `c_emotion_type_8_cnt` UInt64,
    `c_emotion_type_9_cnt` UInt64,
    `s_emotion_type_8_cnt` UInt64,
    `human_check_tag_name_arr` Array(String),
    `human_check_tag_cnt_arr` Array(UInt64),
    `customize_check_tag_name_arr` Array(String),
    `customize_check_tag_cnt_arr` Array(UInt64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/tables/{layer}_{shard}/{table}', '{replica}')
PARTITION BY day
ORDER BY (platform, seller_nick)
SETTINGS index_granularity = 8192, storage_policy = 'rr'

-- 创建临时分布式表
CREATE TABLE tmp.xqc_qc_report_snick_all ON CLUSTER cluster_3s_2r
AS tmp.xqc_qc_report_snick_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'xqc_qc_report_snick_local', rand())

-- 写入数据
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
                sum(arraySum(rule_stats_score)) AS rule_score,
                sum(arraySum(rule_add_stats_score)) AS rule_score_add,
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
            WHERE toYYYYMMDD(begin_time) BETWEEN 20220301 AND 20220331
            AND platform = 'tb'
            AND seller_nick GLOBAL IN (
                -- 查询对应企业-平台的店铺
                SELECT DISTINCT seller_nick
                FROM xqc_dim.xqc_shop_all
                WHERE day=toYYYYMMDD(yesterday())
                AND platform = 'tb'
                AND company_id = '614d86d84eed94e6fc980b1c'
            )
            AND snick GLOBAL IN (
                -- 获取最新版本的维度数据(T+1)
                SELECT distinct snick
                FROM ods.xinghuan_employee_snick_all
                WHERE day = toYYYYMMDD(yesterday())
                AND platform = 'tb'
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
                    WHERE toYYYYMMDD(begin_time) BETWEEN 20220301 AND 20220331
                    AND platform = 'tb'
                    AND seller_nick GLOBAL IN (
                        -- 查询对应企业-平台的店铺
                        SELECT DISTINCT seller_nick
                        FROM xqc_dim.xqc_shop_all
                        WHERE day=toYYYYMMDD(yesterday())
                        AND platform = 'tb'
                        AND company_id = '614d86d84eed94e6fc980b1c'
                    )
                    AND snick GLOBAL IN (
                        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                        -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                        SELECT distinct snick
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = toYYYYMMDD(yesterday())
                        AND platform = 'tb'
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
                    WHERE toYYYYMMDD(begin_time) BETWEEN 20220301 AND 20220331
                    AND platform = 'tb'
                    AND seller_nick GLOBAL IN (
                        -- 查询对应企业-平台的店铺
                        SELECT DISTINCT seller_nick
                        FROM xqc_dim.xqc_shop_all
                        WHERE day=toYYYYMMDD(yesterday())
                        AND platform = 'tb'
                        AND company_id = '614d86d84eed94e6fc980b1c'
                    )
                    AND snick GLOBAL IN (
                        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                        -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                        SELECT distinct snick
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = toYYYYMMDD(yesterday())
                        AND platform = 'tb'
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
                    WHERE toYYYYMMDD(begin_time) BETWEEN 20220301 AND 20220331
                    AND platform = 'tb'
                    AND seller_nick GLOBAL IN (
                        -- 查询对应企业-平台的店铺
                        SELECT DISTINCT seller_nick
                        FROM xqc_dim.xqc_shop_all
                        WHERE day=toYYYYMMDD(yesterday())
                        AND platform = 'tb'
                        AND company_id = '614d86d84eed94e6fc980b1c'
                    )
                    AND snick GLOBAL IN (
                        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                        -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                        SELECT distinct snick
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = toYYYYMMDD(yesterday())
                        AND company_id = '614d86d84eed94e6fc980b1c'
                        AND platform = 'tb'
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
                    WHERE toYYYYMMDD(begin_time) BETWEEN 20220301 AND 20220331
                    AND platform = 'tb'
                    AND seller_nick GLOBAL IN (
                        -- 查询对应企业-平台的店铺
                        SELECT DISTINCT seller_nick
                        FROM xqc_dim.xqc_shop_all
                        WHERE day=toYYYYMMDD(yesterday())
                        AND platform = 'tb'
                        AND company_id = '614d86d84eed94e6fc980b1c'
                    )
                    AND snick GLOBAL IN (
                        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                        -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                        SELECT distinct snick
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = toYYYYMMDD(yesterday())
                        AND company_id = '614d86d84eed94e6fc980b1c'
                        AND platform = 'tb'
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
                WHERE toYYYYMMDD(begin_time) BETWEEN 20220301 AND 20220331
                AND platform = 'tb'
                AND seller_nick GLOBAL IN (
                    -- 查询对应企业-平台的店铺
                    SELECT DISTINCT seller_nick
                    FROM xqc_dim.xqc_shop_all
                    WHERE day=toYYYYMMDD(yesterday())
                    AND platform = 'tb'
                    AND company_id = '614d86d84eed94e6fc980b1c'
                )
                AND snick GLOBAL IN (
                    -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                    -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                    SELECT distinct snick
                    FROM ods.xinghuan_employee_snick_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND platform = 'tb'
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
                WHERE toYYYYMMDD(begin_time) BETWEEN 20220301 AND 20220331
                AND platform = 'tb'
                AND seller_nick GLOBAL IN (
                    -- 查询对应企业-平台的店铺
                    SELECT DISTINCT seller_nick
                    FROM xqc_dim.xqc_shop_all
                    WHERE day=toYYYYMMDD(yesterday())
                    AND platform = 'tb'
                    AND company_id = '614d86d84eed94e6fc980b1c'
                )
                AND snick GLOBAL IN (
                    -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                    -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                    SELECT distinct snick
                    FROM ods.xinghuan_employee_snick_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND platform = 'tb'
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
            -- 人工质检标签维度表
            SELECT
                _id AS tag_id,
                name AS tag_name
            FROM ods.xdqc_tag_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '614d86d84eed94e6fc980b1c'
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
        WHERE toYYYYMMDD(begin_time) BETWEEN 20220301 AND 20220331
        AND platform = 'tb'
        AND seller_nick GLOBAL IN (
            -- 查询对应企业-平台的店铺
            SELECT DISTINCT seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day=toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '614d86d84eed94e6fc980b1c'
        )
        AND snick GLOBAL IN (
            -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
            -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
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
        WHERE toYYYYMMDD(begin_time) BETWEEN 20220301 AND 20220331
        AND platform = 'tb'
        AND seller_nick GLOBAL IN (
            -- 查询对应企业-平台的店铺
            SELECT DISTINCT seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day=toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '614d86d84eed94e6fc980b1c'
        )
        AND snick GLOBAL IN (
            -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
            -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
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
    ) AS customize_check_stat
    GLOBAL LEFT JOIN (
        SELECT
            _id AS tag_id,
            name AS tag_name
        FROM ods.xinghuan_customize_rule_all
        WHERE day = toYYYYMMDD(yesterday())
        AND platform = 'tb'
        AND company_id = '614d86d84eed94e6fc980b1c'
    ) AS customize_tag_info
    USING(tag_id)
    GROUP BY day, platform, seller_nick, snick
) AS customize_check_info
USING(day, platform, seller_nick, snick)

-- 质检报表-客服-AI+人工+自定义质检(顾家月度绩效)
-- 质检报表-客服
-- 统计维度: 平台/店铺/子账号, 下钻维度路径: 平台/店铺/子账号分组/子账号/会话
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
    seller_nick AS `店铺`,
    department_name AS `子账号分组`,
    snick AS `客服子账号`,
    employee_name AS `客服姓名`,
    sum(dialog_cnt) AS `总会话量`,
    round((`总会话量`*100 + sum(score_add)- sum(score))/`总会话量`,2) AS `平均分`,
    -- 质检结果总览-AI质检
    `总会话量` AS `AI质检量`,
    sum(abnormal_dialog_cnt) AS `AI异常会话量`,
    sum(ai_score) AS `AI扣分分值`,
    concat(toString(round((`AI异常会话量` * 100 / `总会话量`), 2)),'%') AS `AI扣分会话比例`,
    sum(excellents_dialog_cnt) AS `AI加分会话量`,
    sum(ai_score_add) AS `AI加分分值`,
    concat(toString(round((`AI加分会话量` * 100 / `总会话量`), 2)),'%') AS `AI加分会话比例`,
    -- 质检结果总览-人工质检
    round((0.9604 * `总会话量`) /(0.0025 * `总会话量` + 0.9604), 0) as `建议抽检量`,
    sum(mark_dialog_cnt) AS `人工抽检量`,
    concat(toString(round((`人工抽检量` * 100 / `总会话量`), 2)),'%') as `抽检比例`,

    sum(tag_score_dialog_cnt) AS `人工扣分会话量`,
    sum(mark_score) AS `人工扣分分值`,
    concat(toString(round((`人工扣分会话量` * 100 / `总会话量`), 2)),'%') AS `人工扣分会话比例`,
    sum(tag_score_add_dialog_cnt) `人工加分会话量`,
    sum(mark_score_add) AS `人工加分分值`,
    concat(toString(round((`人工加分会话量` * 100 / `总会话量`), 2)),'%') AS `人工加分会话比例`,
    -- 质检结果总览-自定义质检
    sum(rule_dialog_cnt) AS `自定义扣分会话量`,
    sum(rule_score) AS `自定义扣分分值`,
    concat(toString(round((`自定义扣分会话量` * 100 / `总会话量`), 2)),'%') AS `自定义扣分会话比例`,
    sum(rule_add_dialog_cnt) AS `自定义加分会话量`,
    sum(rule_score_add) AS `自定义加分分值`,
    concat(toString(round((`自定义加分会话量` * 100 / `总会话量`), 2)),'%') AS `自定义加分会话比例`,
    -- AI质检结果
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
    sum(s_emotion_type_8_cnt) AS `客服骂人`,
   
    -- 人工质检结果
    sumMap(human_check_tag_name_arr, human_check_tag_cnt_arr) AS human_check_tag_cnt_kvs,
    arrayStringConcat(arrayMap(x->toString(x),human_check_tag_cnt_kvs.1),'$$') AS `人工质检标签`,
    arrayStringConcat(arrayMap(x->toString(x),human_check_tag_cnt_kvs.2),'$$') AS `人工质检触发次数`,

    -- 自定义质检结果
    sumMap(customize_check_tag_name_arr, customize_check_tag_cnt_arr) AS customize_check_tag_cnt_kvs,
    arrayStringConcat(arrayMap(x->toString(x),customize_check_tag_cnt_kvs.1),'$$') AS `自定义质检标签`,
    arrayStringConcat(arrayMap(x->toString(x),customize_check_tag_cnt_kvs.2),'$$') AS `自定义质检触发次数`

FROM (
    SELECT *
    FROM tmp.xqc_qc_report_snick_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
    AND platform = 'tb'
    AND seller_nick GLOBAL IN (
        -- 查询对应企业-平台的店铺
        SELECT DISTINCT seller_nick
        FROM xqc_dim.xqc_shop_all
        WHERE day=toYYYYMMDD(yesterday())
        AND platform = 'tb'
        AND company_id = '{{ company_id=614d86d84eed94e6fc980b1c }}'
    )
    AND snick GLOBAL IN (
        -- 获取最新版本的维度数据(T+1)
        SELECT distinct snick
        FROM ods.xinghuan_employee_snick_all
        WHERE day = toYYYYMMDD(yesterday())
        AND platform = 'tb'
        AND company_id = '{{ company_id=614d86d84eed94e6fc980b1c }}'
        -- 下拉框-子账号分组
        AND (
            '{{ department_ids }}'=''
            OR
            department_id IN splitByChar(',','{{ department_ids }}')
        )
    )
    -- 下拉框-店铺名
    AND (
        '{{ seller_nicks }}'=''
        OR
        seller_nick IN splitByChar(',','{{ seller_nicks }}')
    )
    -- 下拉框-子账号
    AND (
        '{{ snicks }}'=''
        OR
        snick IN splitByChar(',','{{ snicks }}')
    )
) AS stat_ai_human_customize_check_info
GLOBAL LEFT JOIN (
    -- 获取最新版本的维度数据(T+1)
    SELECT
        snick, employee_name, department_id, department_name
    FROM (
        SELECT snick, employee_name, department_id
        FROM (
            -- 查询对应企业-平台的所有子账号及其部门ID, 不论其是否绑定员工
            SELECT snick, department_id, employee_id
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=614d86d84eed94e6fc980b1c }}'
        ) AS snick_info
        GLOBAL LEFT JOIN (
            SELECT
                _id AS employee_id, username AS employee_name
            FROM ods.xinghuan_employee_all
            WHERE day = toYYYYMMDD(yesterday())
            AND company_id = '{{ company_id=614d86d84eed94e6fc980b1c }}'
        ) AS employee_info
        USING(employee_id)
    ) AS snick_info
    GLOBAL RIGHT JOIN (
        -- PS: 此处需要JOIN 3次来获取子账号分组的完整路径, 因为子账号分组树高为4
        -- parent_department_id全为空,则代表树层次遍历完毕
        SELECT
            level_1.parent_department_id AS parent_department_id,
            level_2_3_4.department_id AS department_id,
            if(
                level_1.department_id!='', 
                concat(level_1.department_name,'-',level_2_3_4.department_name),
                level_2_3_4.department_name
            ) AS department_name
        FROM (
            SELECT
                level_2.parent_department_id AS parent_department_id,
                level_3_4.department_id AS department_id,
                if(
                    level_2.department_id!='', 
                    concat(level_2.department_name,'-',level_3_4.department_name),
                    level_3_4.department_name
                ) AS department_name
            FROM (
                SELECT
                    level_3.parent_department_id AS parent_department_id,
                    level_4.department_id AS department_id,
                    if(
                        level_3.department_id!='', 
                        concat(level_3.department_name,'-',level_4.department_name),
                        level_4.department_name
                    ) AS department_name
                FROM (
                    SELECT 
                        _id AS department_id,
                        name AS department_name,
                        parent_id AS parent_department_id
                    FROM ods.xinghuan_department_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND company_id = '{{ company_id=614d86d84eed94e6fc980b1c }}'
                    AND (
                        parent_id GLOBAL IN (
                            SELECT DISTINCT
                                _id AS department_id
                            FROM ods.xinghuan_department_all
                            WHERE day = toYYYYMMDD(yesterday())
                            AND company_id = '{{ company_id=614d86d84eed94e6fc980b1c }}'
                        ) -- 清除子账号父分组被删除, 而子分组依旧存在的脏数据
                        OR 
                        parent_id = '' -- 保留顶级分组
                    )
                ) AS level_4
                GLOBAL LEFT JOIN (
                    SELECT 
                        _id AS department_id,
                        name AS department_name,
                        parent_id AS parent_department_id
                    FROM ods.xinghuan_department_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND company_id = '{{ company_id=614d86d84eed94e6fc980b1c }}'
                ) AS level_3
                ON level_4.parent_department_id = level_3.department_id
            ) AS level_3_4
            GLOBAL LEFT JOIN (
                SELECT 
                    _id AS department_id,
                    name AS department_name,
                    parent_id AS parent_department_id
                FROM ods.xinghuan_department_all
                WHERE day = toYYYYMMDD(yesterday())
                AND company_id = '{{ company_id=614d86d84eed94e6fc980b1c }}'
            ) AS level_2
            ON level_3_4.parent_department_id = level_2.department_id
        ) AS level_2_3_4
        GLOBAL LEFT JOIN (
            SELECT 
                _id AS department_id,
                name AS department_name,
                parent_id AS parent_department_id
            FROM ods.xinghuan_department_all
            WHERE day = toYYYYMMDD(yesterday())
            AND company_id = '{{ company_id=614d86d84eed94e6fc980b1c }}'
        ) AS level_1
        ON level_2_3_4.parent_department_id = level_1.department_id
    ) AS department_info
    USING (department_id)
) AS snick_department_map
USING(snick)
-- 下拉框-客服名称
WHERE (
    '{{ usernames }}'=''
    OR
    employee_name IN splitByChar(',','{{ usernames }}')
)
GROUP BY platform, seller_nick, department_id, department_name, snick, employee_name
HAVING department_id!='' -- 清除匹配不上历史分组的子账号
ORDER BY platform, seller_nick, department_name, snick, employee_name