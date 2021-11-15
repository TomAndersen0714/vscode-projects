-- 质检报表-店铺-分组-AI质检-客服和买家情绪统计
SELECT
    seller_nick AS `店铺名`,
    department_name AS `子账号分组名`,
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
        -- 统计买家负面情绪
        SELECT
            toInt32(toYYYYMMDD(begin_time)) AS day,
            seller_nick,
            snick,
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
        WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
        AND platform = '{{platform}}'
        AND snick IN (
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
            AND company_id = '{{ company_id }}'
            AND platform = '{{platform}}'
        )
        AND c_emotion_count!=0
        GROUP BY day,seller_nick,snick
    ) AS cnick_emotion_info
    GLOBAL FULL OUTER JOIN (
        -- 统计客服负面情绪
        SELECT
            toInt32(toYYYYMMDD(begin_time)) AS day,
            seller_nick,
            snick,
            sumIf(s_emotion_count, s_emotion_type=8) AS s_emotion_type_8_cnt
        FROM dwd.xdqc_dialog_all
        ARRAY JOIN
            s_emotion_type,
            s_emotion_count
        WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
        AND platform = '{{platform}}'
        AND snick IN (
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
            AND company_id = '{{ company_id }}'
            AND platform = '{{platform}}'
        )
        AND s_emotion_count!=0
        GROUP BY day,seller_nick,snick
    ) AS snick_emotion_info
    USING (day, seller_nick, snick)
) AS snick_emotion_stat
GLOBAL LEFT JOIN (
    -- 查找子账号和部门之间的映射关系
    SELECT
        day, snick, department_id, department_name
    FROM (
        SELECT day, snick, department_id
        FROM ods.xinghuan_employee_snick_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
        AND company_id = '{{ company_id }}'
        AND platform = '{{platform}}'
    ) AS snick_info
    GLOBAL RIGHT JOIN (
        SELECT day, _id AS department_id, name AS department_name
        FROM ods.xinghuan_department_all
        WHERE day = toYYYYMMDD(today()-1)
        AND company_id = '{{ company_id }}'
    ) AS department_info
    USING department_id
) AS snick_department_info
USING (day,snick)
GROUP BY seller_nick, department_id, department_name
ORDER BY seller_nick, department_name