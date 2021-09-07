SELECT
    concat(
        '****',
        '',
        substringUTF8(snick, lengthUTF8(snick) / 2)
    ) AS `卖家昵称`,
    concat(
        '****',
        '',
        substringUTF8(cnick, lengthUTF8(cnick) / 2)
    ) AS `买家昵称`,
    act AS `动作`,
    msg AS `消息内容`,
    remind_answer AS `机器人提示内容`,
    toString(
        toDateTime(cast(msg_time AS Int64), 'Asia/Shanghai')
    ) AS `客户端时间`,
    create_time AS `创建时间`,
    plat_goods_id AS `当前商品`,
    question AS `问题分类`,
    question_b_qid AS `QID`,
    question_b_proba AS `问题分类概率`,
    current_sale_stage AS `销售阶段`,
    `mode` AS `模式`,
    msg_id AS `msgid`,
    if(
        reason_zh != '',
        concat(reason_zh, '|', sub_reason_zh),
        ''
    ) AS `未回复原因`
FROM (
        SELECT *
        FROM (
                SELECT
                    snick,
                    cnick,
                    act,
                    msg,
                    remind_answer,
                    msg_time,
                    qa_id,
                    plat_goods_id,
                    question_b_qid,
                    question_b_proba,
                    current_sale_stage,
                    `mode`,
                    create_time,
                    msg_id
                FROM ods.xdrs_log_all
                WHERE `day` = {{ day }}
                    AND act != ''
                    AND {{ snick_sql }} {{ cnick_sql }} -- AND snick = ''
                    AND create_time BETWEEN '{{ start_time }}' AND '{{ end_time }}'
            ) t1
            LEFT JOIN (
                SELECT
                    qid AS question_b_qid,
                    question
                FROM ods.question_b
            ) t2 USING question_b_qid
    )
    LEFT JOIN (
        SELECT
            qa_id,
            multiIf(
                reason = 1,
                '通用回复未回复',
                reason = 2,
                '关联商品未回复',
                reason = 3,
                '关联商品类型未回复',
                reason = 4,
                '未达到发送阈值未回复',
                reason = 5,
                '因接待设置未回复',
                '未知'
            ) AS reason_zh,
            reason,
            multiIf(
                sub_reason = 1,
                '辅助模式自动发送关闭',
                sub_reason = 2,
                '无人值守自动发送关闭',
                sub_reason = 3,
                '答案缺失（时效）',
                sub_reason = 4,
                '答案缺失',
                sub_reason = 10,
                '机器人前置接待（客户端）',
                sub_reason = 11,
                '针对特定买家不自动回复（客户端）',
                sub_reason = 12,
                '人工抢答未回复（客户端）',
                sub_reason = 13,
                '相同问题不重复回复',
                sub_reason = 14,
                '仅提示模式未自动回复',
                sub_reason = 15,
                '售后问题不自动回复',
                sub_reason = 16,
                '物流答案耗尽',
                sub_reason = 17,
                '账号一样不回复',
                sub_reason = 18,
                '等待焦点商品链接未回复（客户端）',
                sub_reason = 19,
                '转接后转接前问题不回复（客户端）',
                sub_reason = 30,
                '未开启智能辅助开关（小程序）',
                '未知'
            ) AS sub_reason_zh,
            sub_reason
        FROM ods.xdrs_no_reply_logs_all
        WHERE `day` = {{ day }}
            AND shop_id = '{{ shop_id }}' -- AND snick = ''
            {{ cnick_sql }}
    ) t3 USING qa_id
ORDER BY
    cnick,create_time
 
/* 
当前SQL只做了 question_b 匹配，未做 shop_question 匹配。
即目前只是做了行业问题(行业场景)的匹配,未做店铺自定义问题的匹配,最终目的是获取未回复问题的question_b,
即行业场景问题的中文描述.

问题匹配逻辑正确应该是：
ods.xdrs_log_all:
shop_question_id != "" 代表识别到自定义问题
shop_question_type == "keyword" 代表识别到关键词 （优先级最高 高于 question_b）
shop_question_type != "keyword" 代表识别到句子问题 (优先级低于 question_b)

要求：

1、按天同步shop_question表到CH (√)

2、给出符合正确逻辑SQL，并测试其正确性。
*/

/* 
当前问题的目的在于获取question_b中的question,即行业问题对应的问题描述,即找出未回复问题
对应的行业问题的问题描述.
 */


 -- 创建CH Distributed表: dim.shop_question_all


-- Online ClickHouse Local Table
DROP TABLE IF EXISTS dim.shop_question_local ON CLUSTER cluster_3s_2r
CREATE TABLE IF NOT EXISTS dim.shop_question_local ON CLUSTER cluster_3s_2r(
    `_id` String,
    `shop_id` String,
    `update_time` String,
    `answers` String,
    `create_time` String,
    `is_enabled` String,
    `is_keyword` String,
    `question` String,
    `question_status` String,
    `questions` String,
    `replies` String,
    `source` String,
    `version` Int32
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/dim/tables/{layer}_{shard}/shop_question_local','{replica}'
)
PARTITION BY toYYYYMM(toDate(substr(`update_time`, 1, 19)))
ORDER BY (`_id`,`shop_id`,`update_time`) SETTINGS index_granularity = 8192

-- Online ClickHouse Distributed Table
-- PS: 直接使用Local表的 Schema 来创建Distributed表,不必重复书写表结构
DROP TABLE IF EXISTS dim.shop_question_all ON CLUSTER cluster_3s_2r
CREATE TABLE IF NOT EXISTS dim.shop_question_all ON CLUSTER cluster_3s_2r 
AS dim.shop_question_local 
ENGINE = Distributed('cluster_3s_2r', 'dim', 'shop_question_local', rand())

-- clickhouse-client command
clickhouse-client -m --port 29000
-- Online Airflow Test Task Command
airflow test daily_xdmp_shop_question_mongo2ch truncate_ch_shop_question_tbl_task 2020-08-14



-- Test Table
-- DROP TABLE IF EXISTS tmp.shop_question_local ON CLUSTER cluster_3s_2r
CREATE TABLE IF NOT EXISTS tmp.shop_question_local ON CLUSTER cluster_3s_2r(
    `_id` String,
    `shop_id` String,
    `update_time` String,
    `answers` String,
    `create_time` String,
    `is_enabled` String,
    `is_keyword` String,
    `question` String,
    `question_status` String,
    `questions` String,
    `replies` String,
    `source` String,
    `version` Int32
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/tmp/tables/{layer}_{shard}/shop_question_local','{replica}'
)
PARTITION BY toYYYYMM(toDate(substr(`update_time`, 1, 19)))
ORDER BY (`_id`,`shop_id`,`update_time`) SETTINGS index_granularity = 8192

-- DROP TABLE IF EXISTS tmp.shop_question_all ON CLUSTER cluster_3s_2r
CREATE TABLE IF NOT EXISTS tmp.shop_question_all ON CLUSTER cluster_3s_2r 
AS tmp.shop_question_local 
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'shop_question_local', rand())


-- 修改SQL逻辑
SELECT
    concat(
        '****',
        '',
        substringUTF8(snick, lengthUTF8(snick) / 2)
    ) AS `卖家昵称`,
    concat(
        '****',
        '',
        substringUTF8(cnick, lengthUTF8(cnick) / 2)
    ) AS `买家昵称`,
    act AS `动作`,
    msg AS `消息内容`,
    remind_answer AS `机器人提示内容`,
    toString(
        toDateTime(cast(msg_time AS Int64), 'Asia/Shanghai')
    ) AS `客户端时间`,
    create_time AS `创建时间`,
    plat_goods_id AS `当前商品`,
    question AS `问题分类`,
    question_b_qid AS `QID`,
    question_b_proba AS `问题分类概率`,
    current_sale_stage AS `销售阶段`,
    `mode` AS `模式`,
    msg_id AS `msgid`,
    if(
        reason_zh != '',
        concat(reason_zh, '|', sub_reason_zh),
        ''
    ) AS `未回复原因`
FROM (
        SELECT *
        FROM (
                SELECT
                    snick,
                    cnick,
                    act,
                    msg,
                    remind_answer,
                    msg_time,
                    qa_id,
                    plat_goods_id,
                    question_b_qid,
                    question_b_proba,
                    current_sale_stage,
                    `mode`,
                    create_time,
                    msg_id
                FROM ods.xdrs_log_all
                WHERE `day` = {{ day }}
                    AND act != ''
                    AND {{ snick_sql }} {{ cnick_sql }} -- AND snick = ''
                    AND create_time BETWEEN '{{ start_time }}' AND '{{ end_time }}'
            ) t1
            LEFT JOIN (
                SELECT
                    qid AS question_b_qid,
                    question
                FROM ods.question_b
            ) t2 USING question_b_qid
    )
    LEFT JOIN (
        SELECT
            qa_id,
            multiIf(
                reason = 1,
                '通用回复未回复',
                reason = 2,
                '关联商品未回复',
                reason = 3,
                '关联商品类型未回复',
                reason = 4,
                '未达到发送阈值未回复',
                reason = 5,
                '因接待设置未回复',
                '未知'
            ) AS reason_zh,
            reason,
            multiIf(
                sub_reason = 1,
                '辅助模式自动发送关闭',
                sub_reason = 2,
                '无人值守自动发送关闭',
                sub_reason = 3,
                '答案缺失（时效）',
                sub_reason = 4,
                '答案缺失',
                sub_reason = 10,
                '机器人前置接待（客户端）',
                sub_reason = 11,
                '针对特定买家不自动回复（客户端）',
                sub_reason = 12,
                '人工抢答未回复（客户端）',
                sub_reason = 13,
                '相同问题不重复回复',
                sub_reason = 14,
                '仅提示模式未自动回复',
                sub_reason = 15,
                '售后问题不自动回复',
                sub_reason = 16,
                '物流答案耗尽',
                sub_reason = 17,
                '账号一样不回复',
                sub_reason = 18,
                '等待焦点商品链接未回复（客户端）',
                sub_reason = 19,
                '转接后转接前问题不回复（客户端）',
                sub_reason = 30,
                '未开启智能辅助开关（小程序）',
                '未知'
            ) AS sub_reason_zh,
            sub_reason
        FROM ods.xdrs_no_reply_logs_all
        WHERE `day` = {{ day }}
            AND shop_id = '{{ shop_id }}' -- AND snick = ''
            {{ cnick_sql }}
    ) t3 USING qa_id
ORDER BY
    cnick,create_time

/*
当前SQL只做了 question_b 匹配，未做 shop_question 匹配。
即目前只是做了行业问题(行业场景)的匹配,未做店铺自定义问题的匹配,最终目的是获取未回复问题的question,
即问题的中文描述,也许是店铺自定义关键字匹配问题,也许是默认的行业场景问题,也许是店铺自定义的整句匹配问题.

问题匹配逻辑正确应该是：
ods.xdrs_log_all:
shop_question_id != "" 代表识别到自定义问题
shop_question_type == "keyword" 代表识别到关键词 （优先级最高 高于 question_b）
shop_question_type != "keyword" 代表识别到句子问题 (优先级低于 question_b)

*/

-- 优先级1:通过关键字匹配店铺自定义问题
SELECT *
FROM (
    SELECT
        snick,
        cnick,
        act,
        msg,
        remind_answer,
        msg_time,
        qa_id,
        plat_goods_id,
        shop_question_id,
        question_b_qid,
        question_b_proba,
        current_sale_stage,
        `mode`,
        create_time,
        msg_id
    FROM ods.xdrs_log_all
    WHERE shop_question_id != ''
    AND shop_question_type = 'keyword'
) AS t1
GLOBAL LEFT JOIN (
    SELECT
        _id AS shop_question_id,
        question
    FROM dim.shop_question_all
) AS t2
USING shop_question_id
LIMIT 100

-- 优先级2:匹配默认行业场景问题
SELECT *
FROM (
    SELECT
        snick,
        cnick,
        act,
        msg,
        remind_answer,
        msg_time,
        qa_id,
        plat_goods_id,
        shop_question_id,
        question_b_qid,
        question_b_proba,
        current_sale_stage,
        `mode`,
        create_time,
        msg_id
    FROM ods.xdrs_log_all
    WHERE shop_question_id == ''
) t1
LEFT JOIN (
    SELECT
        qid AS question_b_qid,
        question
    FROM ods.question_b
) t2 USING question_b_qid
LIMIT 100

-- 优先级3:通过整句匹配店铺自定义问题
SELECT *
FROM 
(
    SELECT
        snick,
        cnick,
        act,
        msg,
        remind_answer,
        msg_time,
        qa_id,
        plat_goods_id,
        shop_question_id,
        question_b_qid,
        question_b_proba,
        current_sale_stage,
        `mode`,
        create_time,
        msg_id
    FROM ods.xdrs_log_all
    WHERE shop_question_id != ''
    AND shop_question_type != 'keyword'
) AS t1
GLOBAL LEFT JOIN (
    SELECT
        _id AS shop_question_id,
        question
    FROM dim.shop_question_all
) AS t2
USING shop_question_id
LIMIT 100

-- 最终版
SELECT concat(
        '****',
        '',
        substringUTF8(snick, lengthUTF8(snick) / 2)
    ) AS `卖家昵称`,
    concat(
        '****',
        '',
        substringUTF8(cnick, lengthUTF8(cnick) / 2)
    ) AS `买家昵称`,
    act AS `动作`,
    msg AS `消息内容`,
    remind_answer AS `机器人提示内容`,
    toString(
        toDateTime(cast(msg_time AS Int64), 'Asia/Shanghai')
    ) AS `客户端时间`,
    create_time AS `创建时间`,
    plat_goods_id AS `当前商品`,
    question AS `问题分类`,
    question_b_qid AS `QID`,
    question_b_proba AS `问题分类概率`,
    current_sale_stage AS `销售阶段`,
    `mode` AS `模式`,
    msg_id AS `msgid`,
    if(
        reason_zh != '',
        concat(reason_zh, '|', sub_reason_zh),
        ''
    ) AS `未回复原因`
FROM (
        -- 优先级1:通过关键字匹配店铺自定义问题
        SELECT *
        FROM (
                SELECT snick,
                    cnick,
                    act,
                    msg,
                    remind_answer,
                    msg_time,
                    qa_id,
                    plat_goods_id,
                    shop_question_id,
                    question_b_qid,
                    question_b_proba,
                    current_sale_stage,
                    `mode`,
                    create_time,
                    msg_id
                FROM ods.xdrs_log_all
                WHERE shop_question_id != ''
                    AND shop_question_type = 'keyword'
                    AND `day` = {{ day }}
                    AND act != ''
                    AND {{ snick_sql }} {{ cnick_sql }} -- AND snick = ''
                    AND create_time BETWEEN '{{ start_time }}' AND '{{ end_time }}'
            ) AS t1 GLOBAL
            LEFT JOIN (
                SELECT _id AS shop_question_id,
                    question
                FROM dim.shop_question_all
            ) AS t2 USING shop_question_id
        UNION ALL
        -- 优先级2:匹配默认行业场景问题
        SELECT *
        FROM (
                SELECT snick,
                    cnick,
                    act,
                    msg,
                    remind_answer,
                    msg_time,
                    qa_id,
                    plat_goods_id,
                    shop_question_id,
                    question_b_qid,
                    question_b_proba,
                    current_sale_stage,
                    `mode`,
                    create_time,
                    msg_id
                FROM ods.xdrs_log_all
                WHERE shop_question_id = ''
                    AND `day` = {{ day }}
                    AND act != ''
                    AND {{ snick_sql }} {{ cnick_sql }} -- AND snick = ''
                    AND create_time BETWEEN '{{ start_time }}' AND '{{ end_time }}'
            ) t1
            LEFT JOIN (
                SELECT qid AS question_b_qid,
                    question
                FROM ods.question_b
            ) t2 USING question_b_qid
        UNION ALL
        -- 优先级3:通过整句匹配店铺自定义问题
        SELECT *
        FROM (
                SELECT snick,
                    cnick,
                    act,
                    msg,
                    remind_answer,
                    msg_time,
                    qa_id,
                    plat_goods_id,
                    shop_question_id,
                    question_b_qid,
                    question_b_proba,
                    current_sale_stage,
                    `mode`,
                    create_time,
                    msg_id
                FROM ods.xdrs_log_all
                WHERE shop_question_id != ''
                    AND shop_question_type != 'keyword'
                    AND `day` = {{ day }}
                    AND act != ''
                    AND {{ snick_sql }} {{ cnick_sql }} -- AND snick = ''
                    AND create_time BETWEEN '{{ start_time }}' AND '{{ end_time }}'
            ) AS t1 GLOBAL
            LEFT JOIN (
                SELECT _id AS shop_question_id,
                    question
                FROM dim.shop_question_all
            ) AS t2 USING shop_question_id
    )
    LEFT JOIN (
        SELECT qa_id,
            multiIf(
                reason = 1,
                '通用回复未回复',
                reason = 2,
                '关联商品未回复',
                reason = 3,
                '关联商品类型未回复',
                reason = 4,
                '未达到发送阈值未回复',
                reason = 5,
                '因接待设置未回复',
                '未知'
            ) AS reason_zh,
            reason,
            multiIf(
                sub_reason = 1,
                '辅助模式自动发送关闭',
                sub_reason = 2,
                '无人值守自动发送关闭',
                sub_reason = 3,
                '答案缺失（时效）',
                sub_reason = 4,
                '答案缺失',
                sub_reason = 10,
                '机器人前置接待（客户端）',
                sub_reason = 11,
                '针对特定买家不自动回复（客户端）',
                sub_reason = 12,
                '人工抢答未回复（客户端）',
                sub_reason = 13,
                '相同问题不重复回复',
                sub_reason = 14,
                '仅提示模式未自动回复',
                sub_reason = 15,
                '售后问题不自动回复',
                sub_reason = 16,
                '物流答案耗尽',
                sub_reason = 17,
                '账号一样不回复',
                sub_reason = 18,
                '等待焦点商品链接未回复（客户端）',
                sub_reason = 19,
                '转接后转接前问题不回复（客户端）',
                sub_reason = 30,
                '未开启智能辅助开关（小程序）',
                '未知'
            ) AS sub_reason_zh,
            sub_reason
        FROM ods.xdrs_no_reply_logs_all
        WHERE `day` = {{ day }}
            AND shop_id = '{{ shop_id }}' -- AND snick = ''
            {{ cnick_sql }}
    ) t3 USING qa_id
ORDER BY cnick,
    create_time