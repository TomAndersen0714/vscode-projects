SELECT 
    seller_nick, 
    department_id,
    department_name,
    sumIf(abnormal_cnt, abnormal_type=1) AS `非客服结束会话`,
    sumIf(abnormal_cnt, abnormal_type=2) AS `漏跟进`,
    sumIf(abnormal_cnt, abnormal_type=3) AS `快捷短语重复`,
    sumIf(abnormal_cnt, abnormal_type=4) AS `生硬拒绝`,
    sumIf(abnormal_cnt, abnormal_type=5) AS `欠缺安抚`,
    sumIf(abnormal_cnt, abnormal_type=6) AS `答非所问`,
    sumIf(abnormal_cnt, abnormal_type=7) AS `单字回复`,
    sumIf(abnormal_cnt, abnormal_type=8) AS `单句响应慢`,
    sumIf(abnormal_cnt, abnormal_type=9) AS `产品不熟悉`,
    sumIf(abnormal_cnt, abnormal_type=10) AS `活动不熟悉`,
    sumIf(abnormal_cnt, abnormal_type=11) AS `内部回复慢`,
    sumIf(abnormal_cnt, abnormal_type=12) AS `回复严重超时`,
    sumIf(abnormal_cnt, abnormal_type=13) AS `撤回人工消息`,
    sumIf(abnormal_cnt, abnormal_type=14) AS `单表情回复`,
    sumIf(abnormal_cnt, abnormal_type=15) AS `异常撤回`,
    sumIf(abnormal_cnt, abnormal_type=16) AS `转接前未有效回复`,
    sumIf(abnormal_cnt, abnormal_type=17) AS `超时未回复`,
    sumIf(abnormal_cnt, abnormal_type=18) AS `顾客撤回`,
    sumIf(abnormal_cnt, abnormal_type=19) AS `前后回复矛盾`,
    sumIf(abnormal_cnt, abnormal_type=20) AS `撤回机器人消息`,
    sumIf(abnormal_cnt, abnormal_type=21) AS `第三方投诉或曝光`,
    sumIf(abnormal_cnt, abnormal_type=22) AS `顾客提及投诉或举报`,
    sumIf(abnormal_cnt, abnormal_type=23) AS `差评或要挟差评`,
    sumIf(abnormal_cnt, abnormal_type=24) AS `反问/质疑顾客`,
    sumIf(abnormal_cnt, abnormal_type=25) AS `违禁词`,
    sumIf(abnormal_cnt, abnormal_type=26) AS `客服冷漠讥讽`,
    sumIf(abnormal_cnt, abnormal_type=27) AS `顾客怀疑假货`,
    sumIf(abnormal_cnt, abnormal_type=28) AS `客服态度消极敷衍`,
    sumIf(abnormal_cnt, abnormal_type=29) AS `售后不满意`
FROM (
    SELECT
        day,
        seller_nick,
        snick,
        abnormal_type,
        abnormal_cnt
    FROM (
        SELECT
            toInt32(toYYYYMMDD(begin_time)) AS day,
            seller_nick,
            snick,
            abnormals_type,
            abnormals_count
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
        AND snick GLOBAL IN (
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(today()-1)
            AND company_id = '{{ company_id }}'
        )
    )
    ARRAY JOIN
        abnormals_type AS abnormal_type, 
        abnormals_count AS abnormal_cnt
    WHERE abnormal_cnt!=0
) AS ai_abnormal_info
GLOBAL LEFT JOIN (
    SELECT
        day, snick, department_id, department_name
    FROM (
        SELECT day, snick, department_id
        FROM ods.xinghuan_employee_snick_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
        AND company_id = '{{ company_id }}'
        AND platform = 'jd'
    ) AS snick_info
    GLOBAL LEFT JOIN (
        SELECT day, _id AS department_id, name AS department_name
        FROM ods.xinghuan_department_all
        WHERE day = toYYYYMMDD(today()-1)
        AND company_id = '{{ company_id }}'
    ) AS department_info
    USING (day, department_id)
) AS snick_department_map
USING(day, snick)
GROUP BY seller_nick, department_id, department_name
HAVING department_id!=''
ORDER BY seller_nick, department_name