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
-- 下拉框-客服姓名
WHERE (
    '{{ usernames }}'=''
    OR
    employee_name IN splitByChar(',','{{ usernames }}')
)
GROUP BY platform, seller_nick, department_id, department_name, snick, employee_name
HAVING department_id!='' -- 清除匹配不上历史分组的子账号
ORDER BY platform, seller_nick, department_name, snick, employee_name