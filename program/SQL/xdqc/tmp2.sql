SELECT COUNT(1) FROM dwd.xdqc_dialog_all
WHERE seller_nick = '方太官方旗舰店'
AND toYYYYMMDD(begin_time) BETWEEN 20211103 AND 20211106

-- AI质检项Id映射关系
var abnormalTypeStrings = map[int]string{
	1:   "非客服结束会话",
	2:   "漏跟进",
	3:   "快捷语重复",
	4:   "生硬拒绝",
	5:   "欠缺安抚",
	6:   "答非所问",
	7:   "单字回复",
	8:   "单句响应慢",
	9:   "产品不熟悉",
	10:  "活动不熟悉",
	11:  "内部回复慢",
	12:  "严重超时",
	13:  "撤回消息",
	14:  "单表情回复",
	15:  "异常撤回",
	16:  "转接前未有效回复",
	17:  "超时未回复",
	18:  "顾客撤回",
	19:  "前后回复矛盾",
	20:  "撤回机器人消息",
	21:  "第三方投诉或曝光",
	22:  "顾客提及投诉或举报",
	23:  "差评或要挟差评",
	24:  "反问/质疑顾客",
	25:  "违禁词",
	26:  "客服冷漠讥讽",
	27:  "顾客怀疑假货",
	28:  "客服态度消极敷衍",
	29:  "售后不满意",
	100: "疑似非客服结束会话",
}

SELECT
    toDate('{ds}') AS `day`,
    dialog_info.platform,
    dialog_info.seller_nick AS seller_nick,
    dialog_info.`group` AS group,
    dialog_info.snick AS snick,
    dialog_info._id AS _id, -- 会话ID
    dialog_info.cnick,
    dialog_info.mark,
    arraySum(dialog_info.abnormals_count) AS abnormals_count, -- AI质检质检扣分次数
    arraySum(dialog_info.excellents_count) AS excellents_count, -- AI质检质检加分次数
    length(dialog_info.read_mark) AS read_mark_count, -- 被进行人工质检的次数
    dialog_info.score,
    dialog_info.score_add,
    dialog_info.mark_score,
    dialog_info.mark_score_add,
    employee_info.username AS username,
    dialog_info.abnormals_count [1] AS `非客服结束会话`,
    dialog_info.abnormals_count [2] AS `漏跟进`,
    dialog_info.abnormals_count [3] AS `快捷语重复`,
    dialog_info.abnormals_count [4] AS `生硬拒绝`,
    dialog_info.abnormals_count [5] AS `欠缺安抚`,
    dialog_info.abnormals_count [6] AS `答非所问`,
    dialog_info.abnormals_count [7] AS `单字回复`,
    dialog_info.abnormals_count [8] AS `单句响应慢`,
    dialog_info.abnormals_count [9] AS `产品不熟悉`,
    dialog_info.abnormals_count [10] AS `活动不熟悉`,
    dialog_info.abnormals_count [11] AS `内部回复慢`,
    dialog_info.abnormals_count [12] AS `严重超时`,
    dialog_info.abnormals_count [13] AS `撤回消息`,
    dialog_info.abnormals_count [14] AS `单表情回复`,
    dialog_info.abnormals_count [15] AS `异常撤回`,
    dialog_info.abnormals_count [16] AS `转接前未有效回复`,
    dialog_info.abnormals_count [17] AS `超时未回复`,
    dialog_info.abnormals_count [18] AS `顾客撤回`,
    dialog_info.abnormals_count [19] AS `前后回复矛盾`,
    dialog_info.abnormals_count [20] AS `撤回机器人消息`,
    dialog_info.abnormals_count [21] AS `第三方投诉或曝光`,
    dialog_info.abnormals_count [22] AS `顾客提及投诉或举报`,
    dialog_info.abnormals_count [23] AS `差评或要挟差评`,
    dialog_info.abnormals_count [24] AS `反问/质疑顾客`,
    dialog_info.abnormals_count [25] AS `违禁词`,
    dialog_info.abnormals_count [26] AS `客服冷漠讥讽`,
    dialog_info.abnormals_count [27] AS `顾客怀疑假货`,
    dialog_info.abnormals_count [28] AS `客服态度消极敷衍`,
    dialog_info.abnormals_count [29] AS `售后不满意`,
    dialog_info.tag_score_stats_id AS tag_score_stats_id, -- 人工质检扣分标签ID数组
    dialog_info.tag_score_stats_score AS tag_score_stats_score, -- 人工质检标签扣分数值对应数组
    dialog_info.tag_score_add_stats_id AS tag_score_add_stats_id, -- 人工质检加分标签ID数组
    dialog_info.tag_score_add_stats_score AS tag_score_add_stats_score, -- 人工质检标签扣分数值对应数组
    dialog_info.rule_stats_id AS rule_stats_id, -- 自定义质检中扣分质检项的ID数组
    dialog_info.rule_stats_score AS rule_stats_score, -- 自定义质检项中扣分质检项对应分值
    dialog_info.rule_stats_count AS rule_stats_count, -- 自定义质检项中扣分质检项各自触发的次数
    dialog_info.rule_add_stats_id AS rule_add_stats_id, -- 自定义质检项中加分质检项ID数组
    dialog_info.rule_add_stats_score AS rule_add_stats_score, -- 自定义质检项中加分质检项对应分值
    dialog_info.rule_add_stats_count AS rule_add_stats_count -- 自定义质检项中加分质检项对应的触发次数数组
FROM (
        SELECT *
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) = { ds_nodash }
        AND seller_nick = { shop_name }
        AND platform = { platform }
) AS dialog_info
GLOBAL LEFT JOIN (
    SELECT 
        account_info.account_id AS account_id,
        employee_info.username AS username
    FROM (
        SELECT
            _id AS account_id,
            employee_id
        FROM ods.xinghuan_account_all
        WHERE day = { ds_nodash }
        AND company_id = { company_id }
    ) AS account_info GLOBAL
    GLOBAL LEFT JOIN (
        SELECT
            _id AS employee_id,
            username
        FROM ods.xinghuan_employee_all
        WHERE day = { ds_nodash }
        AND company_id = { company_id }
    ) AS employee_info 
    using(employee_id)
) AS employee_info
ON dialog_info.last_mark_id = employee_info.account_id




-- 查询员工AI质检项触发次数
-- company_id = '5f747ba42c90fd0001254404'

SELECT
    employee_id,
    username,
    snick,
    20211106 AS day
FROM (
    SELECT *
    FROM ods.xinghuan_employee_snick_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
    AND company_id = '5f747ba42c90fd0001254404'
) AS snick_info
GLOBAL RIGHT JOIN (
    SELECT 
        _id AS employee_id, 
        username,
        day
    FROM ods.xinghuan_employee_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
    AND company_id = '5f747ba42c90fd0001254404'
) AS employee_info
ON snick_info.employee_id = employee_info.employee_id
AND snick_info.day = employee_info.day

-- 方太 company_id = '5f747ba42c90fd0001254404'
SELECT 
    day AS `日期`,
    seller_nick AS `店铺`,
    employee_id AS `员工ID`,
    employee_name AS `员工姓名`,
    sum(cnt_sum) AS `AI质检项总触发次数`,
    sum(type_1) AS `非客服结束会话`,
    sum(type_2) AS `漏跟进`,
    sum(type_3) AS `快捷语重复`,
    sum(type_4) AS `生硬拒绝`,
    sum(type_5) AS `欠缺安抚`,
    sum(type_6) AS `答非所问`,
    sum(type_7) AS `单字回复`,
    sum(type_8) AS `单句响应慢`,
    sum(type_9) AS `产品不熟悉`,
    sum(type_10) AS `活动不熟悉`,
    sum(type_11) AS `内部回复慢`,
    sum(type_12) AS `严重超时`,
    sum(type_13) AS `撤回消息`,
    sum(type_14) AS `单表情回复`,
    sum(type_15) AS `异常撤回`,
    sum(type_16) AS `转接前未有效回复`,
    sum(type_17) AS `超时未回复`,
    sum(type_18) AS `顾客撤回`,
    sum(type_19) AS `前后回复矛盾`,
    sum(type_20) AS `撤回机器人消息`,
    sum(type_21) AS `第三方投诉或曝光`,
    sum(type_22) AS `顾客提及投诉或举报`,
    sum(type_23) AS `差评或要挟差评`,
    sum(type_24) AS `反问/质疑顾客`,
    sum(type_25) AS `违禁词`,
    sum(type_26) AS `客服冷漠讥讽`,
    sum(type_27) AS `顾客怀疑假货`,
    sum(type_28) AS `客服态度消极敷衍`,
    sum(type_29) AS `售后不满意`
FROM (
    SELECT
        day,
        dialog_info.seller_nick AS seller_nick,
        snick_employee_info.employee_id AS employee_id,
        snick_employee_info.employee_name AS employee_name,
        dialog_info.abnormals_count [1] AS type_1,
        dialog_info.abnormals_count [2] AS type_2,
        dialog_info.abnormals_count [3] AS type_3,
        dialog_info.abnormals_count [4] AS type_4,
        dialog_info.abnormals_count [5] AS type_5,
        dialog_info.abnormals_count [6] AS type_6,
        dialog_info.abnormals_count [7] AS type_7,
        dialog_info.abnormals_count [8] AS type_8,
        dialog_info.abnormals_count [9] AS type_9,
        dialog_info.abnormals_count [10] AS type_10,
        dialog_info.abnormals_count [11] AS type_11,
        dialog_info.abnormals_count [12] AS type_12,
        dialog_info.abnormals_count [13] AS type_13,
        dialog_info.abnormals_count [14] AS type_14,
        dialog_info.abnormals_count [15] AS type_15,
        dialog_info.abnormals_count [16] AS type_16,
        dialog_info.abnormals_count [17] AS type_17,
        dialog_info.abnormals_count [18] AS type_18,
        dialog_info.abnormals_count [19] AS type_19,
        dialog_info.abnormals_count [20] AS type_20,
        dialog_info.abnormals_count [21] AS type_21,
        dialog_info.abnormals_count [22] AS type_22,
        dialog_info.abnormals_count [23] AS type_23,
        dialog_info.abnormals_count [24] AS type_24,
        dialog_info.abnormals_count [25] AS type_25,
        dialog_info.abnormals_count [26] AS type_26,
        dialog_info.abnormals_count [27] AS type_27,
        dialog_info.abnormals_count [28] AS type_28,
        dialog_info.abnormals_count [29] AS type_29,
        arraySum(dialog_info.abnormals_count) AS cnt_sum
    FROM (
            SELECT *, toInt32(toYYYYMMDD(begin_time)) AS day
            FROM dwd.xdqc_dialog_all
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
            AND snick IN (
                SELECT distinct snick
                FROM ods.xinghuan_employee_snick_all
                WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
                AND company_id = '{{ company_id }}'
            )
    ) AS dialog_info
    GLOBAL RIGHT JOIN (
        SELECT
            day,
            employee_id,
            employee_info.employee_name AS employee_name,
            snick_info.snick
        FROM (
            SELECT *
            FROM ods.xinghuan_employee_snick_all
            WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
            AND company_id = '{{ company_id }}'
        ) AS snick_info
        GLOBAL RIGHT JOIN (
            SELECT
                day,
                _id AS employee_id, 
                username AS employee_name
            FROM ods.xinghuan_employee_all
            WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
            AND company_id = '{{ company_id }}'
        ) AS employee_info
        USING(employee_id, day)
    ) AS snick_employee_info
    USING(snick, day)
)
GROUP BY day, seller_nick, employee_id, employee_name
ORDER BY day ASC, `AI质检项总触发次数` DESC




    SELECT
        count(1)
    FROM (
            SELECT *, toInt32(toYYYYMMDD(begin_time)) AS day
            FROM dwd.xdqc_dialog_all
            WHERE toYYYYMMDD(begin_time) BETWEEN 20211106 AND 20211106
            AND snick IN (
                SELECT distinct snick
                FROM ods.xinghuan_employee_snick_all
                WHERE day BETWEEN 20211106 AND 20211106
                AND company_id = '5f747ba42c90fd0001254404'
            )
    ) AS dialog_info
    GLOBAL RIGHT JOIN (
        SELECT
            day,
            employee_id,
            employee_info.employee_name AS employee_name,
            snick_info.snick
        FROM (
            SELECT *
            FROM ods.xinghuan_employee_snick_all
            WHERE day BETWEEN 20211106 AND 20211106
            AND company_id = '5f747ba42c90fd0001254404'
        ) AS snick_info
        GLOBAL RIGHT JOIN (
            SELECT
                day,
                _id AS employee_id, 
                username AS employee_name
            FROM ods.xinghuan_employee_all
            WHERE day BETWEEN 20211106 AND 20211106
            AND company_id = '5f747ba42c90fd0001254404'
        ) AS employee_info
        USING(employee_id, day)
    ) AS snick_employee_info
    USING(snick, day)