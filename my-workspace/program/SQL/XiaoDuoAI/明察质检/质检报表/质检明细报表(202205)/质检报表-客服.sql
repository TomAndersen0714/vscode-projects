-- 质检报表-客服
-- 统计维度: 平台/店铺/子账号分组/子账号-客服
-- 下钻维度路径: 平台/店铺/子账号分组/子账号-客服/会话

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
    seller_nick AS `店铺`, department_name AS `子账号分组`, snick AS `客服子账号`,
    employee_name AS `客服姓名`, superior_name AS `上级姓名`,

    dialog_cnt AS `总会话量`,
    round((`总会话量`*100 + add_score_sum- subtract_score_sum)/`总会话量`,2) AS `平均分`,
    -- 质检结果总览-AI质检
    `总会话量` AS `AI质检量`,
    ai_subtract_score_dialog_cnt AS `AI异常会话量`,
    ai_subtract_score_sum AS `AI扣分分值`,
    concat(toString(round((`AI异常会话量` * 100 / `总会话量`), 2)),'%') AS `AI扣分会话比例`,
    ai_add_score_dialog_cnt AS `AI加分会话量`,
    ai_add_score_sum AS `AI加分分值`,
    concat(toString(round((`AI加分会话量` * 100 / `总会话量`), 2)),'%') AS `AI加分会话比例`,
    -- 质检结果总览-人工质检
    round((0.9604 * `总会话量`) /(0.0025 * `总会话量` + 0.9604), 0) as `建议抽检量`,
    manual_marked_dialog_cnt AS `人工抽检量`,
    concat(toString(round((`人工抽检量` * 100 / `总会话量`), 2)),'%') as `抽检比例`,

    manual_subtract_score_dialog_cnt AS `人工扣分会话量`,
    manual_subtract_score_sum AS `人工扣分分值`,
    concat(toString(round((`人工扣分会话量` * 100 / `总会话量`), 2)),'%') AS `人工扣分会话比例`,
    manual_add_score_dialog_cnt `人工加分会话量`,
    manual_add_score_sum AS `人工加分分值`,
    concat(toString(round((`人工加分会话量` * 100 / `总会话量`), 2)),'%') AS `人工加分会话比例`,
    -- 质检结果总览-自定义质检
    custom_subtract_score_dialog_cnt AS `自定义扣分会话量`,
    custom_subtract_score_sum AS `自定义扣分分值`,
    concat(toString(round((`自定义扣分会话量` * 100 / `总会话量`), 2)),'%') AS `自定义扣分会话比例`,
    custom_add_score_dialog_cnt AS `自定义加分会话量`,
    custom_add_score_sum AS `自定义加分分值`,
    concat(toString(round((`自定义加分会话量` * 100 / `总会话量`), 2)),'%') AS `自定义加分会话比例`,

    -- AI质检结果
    arrayStringConcat(arrayMap(x->toString(x), ai_tag_names),'$$') AS `AI质检标签`,
    arrayStringConcat(arrayMap(x->toString(x), ai_tag_cnts),'$$') AS `AI质检触发次数`,

    -- 人工质检结果
    arrayStringConcat(arrayMap(x->toString(x), manual_tag_names),'$$') AS `人工质检标签`,
    arrayStringConcat(arrayMap(x->toString(x), manual_tag_cnts),'$$') AS `人工质检触发次数`,

    -- 自定义质检结果
    arrayStringConcat(arrayMap(x->toString(x), custom_tag_names),'$$') AS `自定义质检标签`,
    arrayStringConcat(arrayMap(x->toString(x), custom_tag_cnts),'$$') AS `自定义质检触发次数`

FROM (
    SELECT *
    FROM (
        -- 质检结果总览-子账号维度
        SELECT
            platform, seller_nick, snick,
            -- 分值统计-总计
            sum(subtract_score_sum) AS subtract_score_sum,
            sum(add_score_sum) AS add_score_sum,
            -- 分值统计-人工质检
            sum(manual_subtract_score_sum) AS manual_subtract_score_sum,
            sum(manual_add_score_sum) AS manual_add_score_sum,
            -- 分值统计-自定义质检
            sum(custom_subtract_score_sum) AS custom_subtract_score_sum,
            sum(custom_add_score_sum) AS custom_add_score_sum,
            -- 分值统计-AI质检
            subtract_score_sum - manual_subtract_score_sum - custom_subtract_score_sum AS ai_subtract_score_sum,
            add_score_sum - manual_add_score_sum - custom_add_score_sum AS ai_add_score_sum,

            -- 会话量统计-总计
            sum(dialog_cnt) AS dialog_cnt,
            -- 会话量统计-AI质检
            sum(ai_subtract_score_dialog_cnt) AS ai_subtract_score_dialog_cnt,
            sum(ai_add_score_dialog_cnt) AS ai_add_score_dialog_cnt,
            -- 会话量统计-自定义质检
            sum(custom_subtract_score_dialog_cnt) AS custom_subtract_score_dialog_cnt,
            sum(custom_add_score_dialog_cnt) AS custom_add_score_dialog_cnt,
            -- 会话量统计-人工质检
            sum(manual_marked_dialog_cnt) AS manual_marked_dialog_cnt,
            sum(manual_subtract_score_dialog_cnt) AS manual_subtract_score_dialog_cnt,
            sum(manual_add_score_dialog_cnt) AS manual_add_score_dialog_cnt
        FROM xqc_dws.snick_stat_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
        AND platform = 'tb'
        AND seller_nick GLOBAL IN (
            -- 查询对应企业-平台的店铺
            SELECT DISTINCT seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day=toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        )
        AND snick GLOBAL IN (
            -- 获取最新版本的维度数据(T+1)
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
        GROUP BY platform, seller_nick, snick
    ) AS dws_snick_dialog_stat

    GLOBAL FULL OUTER JOIN (
        --质检结果明细-子账号维度
        -- PS: 此处应该先进行预聚合, 减小中间结果的数组长度
        SELECT
            platform, seller_nick, snick,
            -- AI质检
            groupArrayIf(
                tag_name, tag_type IN ['ai_abnormal', 'ai_excellent', 'ai_s_emotion', 'ai_c_emotion']
            ) AS ai_tag_names,
            groupArrayIf(
                tag_cnt_sum, tag_type IN ['ai_abnormal', 'ai_excellent', 'ai_s_emotion', 'ai_c_emotion']
            ) AS ai_tag_cnts,
            -- 人工质检
            groupArrayIf(
                tag_name, tag_type IN ['manual_subtract', 'manual_add']
            ) AS manual_tag_names,
            groupArrayIf(
                tag_cnt_sum, tag_type IN ['manual_subtract', 'manual_add']
            ) AS manual_tag_cnts,
            -- 自定义质检
            groupArrayIf(
                tag_name, tag_type IN ['custom_subtract', 'custom_add', 'custom_message', 'custom_dialog']
            ) AS custom_tag_names,
            groupArrayIf(
                tag_cnt_sum, tag_type IN ['custom_subtract', 'custom_add', 'custom_message', 'custom_dialog']
            ) AS custom_tag_cnts
        FROM (
            SELECT
                platform, seller_nick, snick, tag_type, tag_id, tag_name,
                sum(tag_cnt_sum) AS tag_cnt_sum
            FROM xqc_dws.tag_stat_all
            WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
            AND platform = 'tb'
            AND seller_nick GLOBAL IN (
                -- 查询对应企业-平台的店铺
                SELECT DISTINCT seller_nick
                FROM xqc_dim.xqc_shop_all
                WHERE day=toYYYYMMDD(yesterday())
                AND platform = 'tb'
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            )
            AND snick GLOBAL IN (
                -- 获取最新版本的维度数据(T+1)
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
            GROUP BY platform, seller_nick, snick, tag_type, tag_id, tag_name
        ) AS dws_tag_stat
        GROUP BY platform, seller_nick, snick
    ) AS dws_snick_tag_stat
    USING(platform, seller_nick, snick)
) AS dws_dialog_stat
GLOBAL LEFT JOIN (
    -- 关联子账号分组/子账号员工信息
    SELECT
        snick, employee_name, superior_name, department_id, department_name
    FROM (
        SELECT snick, employee_name, superior_name, department_id
        FROM (
            -- 查询对应企业-平台的所有子账号及其部门ID, 不论其是否绑定员工
            SELECT snick, department_id, employee_id
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        ) AS snick_info
        GLOBAL LEFT JOIN (
            SELECT
                _id AS employee_id, username AS employee_name, superior_name
            FROM ods.xinghuan_employee_all
            WHERE day = toYYYYMMDD(yesterday())
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        ) AS employee_info
        USING(employee_id)
    ) AS snick_info
    GLOBAL RIGHT JOIN (
        SELECT
            _id AS department_id, full_name AS department_name
        FROM xqc_dim.snick_department_full_all
        WHERE day = toYYYYMMDD(yesterday())
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    ) AS department_info
    USING (department_id)
) AS dim_snick_department
USING(snick)
-- 下拉框-客服姓名
WHERE (
    '{{ usernames }}'=''
    OR
    employee_name IN splitByChar(',','{{ usernames }}')
)
-- 清除匹配不上历史分组的子账号
AND department_id!=''
ORDER BY platform, seller_nick, department_name, snick, employee_name