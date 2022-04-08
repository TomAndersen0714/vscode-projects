-- 质检报表-客服-查看详情-AI质检明细
-- 统计维度: 平台/店铺/子账号, 下钻维度路径: 平台/店铺/子账号分组/子账号/会话
SELECT
    dialog_id,
    toYYYYMMDD(begin_time) AS dialog_day,
    dialog_day AS `日期`,
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
    cnick AS `顾客名称`,
    employee_name AS `客服姓名`,
    superior_name AS `上级姓名`,
    
    -- AI质检-扣分质检项触发次数统计
    abnormal_type_1_cnt AS `非客服结束会话`,
    abnormal_type_2_cnt AS `漏跟进`,
    abnormal_type_3_cnt AS `快捷短语重复`,
    abnormal_type_4_cnt AS `生硬拒绝`,
    abnormal_type_5_cnt AS `欠缺安抚`,
    abnormal_type_6_cnt AS `答非所问`,
    abnormal_type_7_cnt AS `单字回复`,
    abnormal_type_8_cnt AS `单句响应慢`,
    abnormal_type_9_cnt AS `产品不熟悉`,
    abnormal_type_10_cnt AS `活动不熟悉`,
    abnormal_type_11_cnt AS `内部回复慢`,
    abnormal_type_12_cnt AS `回复严重超时`,
    abnormal_type_13_cnt AS `撤回人工消息`,
    abnormal_type_14_cnt AS `单表情回复`,
    abnormal_type_15_cnt AS `异常撤回`,
    abnormal_type_16_cnt AS `转接前未有效回复`,
    abnormal_type_17_cnt AS `超时未回复`,
    abnormal_type_18_cnt AS `顾客撤回`,
    abnormal_type_19_cnt AS `前后回复矛盾`,
    abnormal_type_20_cnt AS `撤回机器人消息`,
    abnormal_type_21_cnt AS `第三方投诉或曝光`,
    abnormal_type_22_cnt AS `顾客提及投诉或举报`,
    abnormal_type_23_cnt AS `差评或要挟差评`,
    abnormal_type_24_cnt AS `反问/质疑顾客`,
    abnormal_type_25_cnt AS `违禁词`,
    abnormal_type_26_cnt AS `客服冷漠讥讽`,
    abnormal_type_27_cnt AS `顾客怀疑假货`,
    abnormal_type_28_cnt AS `客服态度消极敷衍`,
    abnormal_type_29_cnt AS `售后不满意`,

    -- AI质检-加分质检项触发次数统计
    excellent_type_1_cnt AS `需求挖掘`,
    excellent_type_2_cnt AS `商品细节解答`,
    excellent_type_3_cnt AS `卖点传达`,
    excellent_type_4_cnt AS `商品推荐`,
    excellent_type_5_cnt AS `退换货理由修改`,
    excellent_type_6_cnt AS `主动跟进`,
    excellent_type_7_cnt AS `无货挽回`,
    excellent_type_8_cnt AS `活动传达`,
    excellent_type_9_cnt AS `店铺保障`,
    excellent_type_10_cnt AS `催拍催付`,
    excellent_type_11_cnt AS `核对地址`,
    excellent_type_12_cnt AS `好评引导`,
    excellent_type_13_cnt AS `优秀结束语`,

    -- AI质检-顾客情绪质检项触发次数统计
    c_emotion_type_1_cnt AS `满意`,
    c_emotion_type_2_cnt AS `感激`,
    c_emotion_type_3_cnt AS `期待`,
    c_emotion_type_4_cnt AS `对客服态度不满`,
    c_emotion_type_5_cnt AS `对发货物流不满`,
    c_emotion_type_6_cnt AS `对产品不满`,
    c_emotion_type_7_cnt AS `其他不满意`,
    c_emotion_type_8_cnt AS `顾客骂人`,
    c_emotion_type_9_cnt AS `对收货少件不满`,

    -- AI质检-客服情绪质检项触发次数统计
    s_emotion_type_8_cnt AS `客服骂人`
FROM (
    -- AI质检结果-会话维度质检项触发次数统计
    SELECT
        platform,
        seller_nick,
        snick,
        cnick,
        _id AS dialog_id,
        begin_time,
        abnormals_count[1] AS abnormal_type_1_cnt,
        abnormals_count[2] AS abnormal_type_2_cnt,
        abnormals_count[3] AS abnormal_type_3_cnt,
        abnormals_count[4] AS abnormal_type_4_cnt,
        abnormals_count[5] AS abnormal_type_5_cnt,
        abnormals_count[6] AS abnormal_type_6_cnt,
        abnormals_count[7] AS abnormal_type_7_cnt,
        abnormals_count[8] AS abnormal_type_8_cnt,
        abnormals_count[9] AS abnormal_type_9_cnt,
        abnormals_count[10] AS abnormal_type_10_cnt,
        abnormals_count[11] AS abnormal_type_11_cnt,
        abnormals_count[12] AS abnormal_type_12_cnt,
        abnormals_count[13] AS abnormal_type_13_cnt,
        abnormals_count[14] AS abnormal_type_14_cnt,
        abnormals_count[15] AS abnormal_type_15_cnt,
        abnormals_count[16] AS abnormal_type_16_cnt,
        abnormals_count[17] AS abnormal_type_17_cnt,
        abnormals_count[18] AS abnormal_type_18_cnt,
        abnormals_count[19] AS abnormal_type_19_cnt,
        abnormals_count[20] AS abnormal_type_20_cnt,
        abnormals_count[21] AS abnormal_type_21_cnt,
        abnormals_count[22] AS abnormal_type_22_cnt,
        abnormals_count[23] AS abnormal_type_23_cnt,
        abnormals_count[24] AS abnormal_type_24_cnt,
        abnormals_count[25] AS abnormal_type_25_cnt,
        abnormals_count[26] AS abnormal_type_26_cnt,
        abnormals_count[27] AS abnormal_type_27_cnt,
        abnormals_count[28] AS abnormal_type_28_cnt,
        abnormals_count[29] AS abnormal_type_29_cnt,
        excellents_count[1] AS excellent_type_1_cnt,
        excellents_count[2] AS excellent_type_2_cnt,
        excellents_count[3] AS excellent_type_3_cnt,
        excellents_count[4] AS excellent_type_4_cnt,
        excellents_count[5] AS excellent_type_5_cnt,
        excellents_count[6] AS excellent_type_6_cnt,
        excellents_count[7] AS excellent_type_7_cnt,
        excellents_count[8] AS excellent_type_8_cnt,
        excellents_count[9] AS excellent_type_9_cnt,
        excellents_count[10] AS excellent_type_10_cnt,
        excellents_count[11] AS excellent_type_11_cnt,
        excellents_count[12] AS excellent_type_12_cnt,
        excellents_count[13] AS excellent_type_13_cnt,
        c_emotion_count[1] AS c_emotion_type_1_cnt,
        c_emotion_count[2] AS c_emotion_type_2_cnt,
        c_emotion_count[3] AS c_emotion_type_3_cnt,
        c_emotion_count[4] AS c_emotion_type_4_cnt,
        c_emotion_count[5] AS c_emotion_type_5_cnt,
        c_emotion_count[6] AS c_emotion_type_6_cnt,
        c_emotion_count[7] AS c_emotion_type_7_cnt,
        c_emotion_count[8] AS c_emotion_type_8_cnt,
        c_emotion_count[9] AS c_emotion_type_9_cnt,
        s_emotion_count[1] AS s_emotion_type_8_cnt
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
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
            -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
            -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    )
    -- 排除AI质检未命中标签的会话
    AND (arraySum(abnormals_count)+arraySum(excellents_count)+arraySum(c_emotion_count)+arraySum(s_emotion_count))!=0
    -- 下拉框-店铺名
    AND (
            '{{ seller_nicks }}'=''
            OR
            seller_nick IN splitByChar(',','{{ seller_nicks }}')
    )
    -- 下拉框-子账号
    AND (
            '{{ snicks=null }}'=''
            OR
            snick IN splitByChar(',','{{ snicks=null }}')
    )
) AS stat_ai_check_info
GLOBAL LEFT JOIN (
    -- 获取最新版本的维度数据(T+1)
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
                    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                    AND (
                        parent_id GLOBAL IN (
                            SELECT DISTINCT
                                _id AS department_id
                            FROM ods.xinghuan_department_all
                            WHERE day = toYYYYMMDD(yesterday())
                            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
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
                    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
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
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
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
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
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
ORDER BY begin_time ASC