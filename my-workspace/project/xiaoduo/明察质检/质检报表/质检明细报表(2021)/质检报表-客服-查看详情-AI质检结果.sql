-- 质检报表-客服-查看详情-AI质检结果
-- 统计维度: 平台/店铺/子账号, 下钻维度路径: 平台/店铺/子账号分组/子账号/会话
SELECT
    CASE
        WHEN platform='tb' THEN '淘宝'
        WHEN platform='jd' THEN '京东'
        WHEN platform='ks' THEN '快手'
        WHEN platform='dy' THEN '抖音'
        WHEN platform='pdd' THEN '拼多多'
        WHEN platform='wx' THEN '企微'
        WHEN platform='open' THEN '开放平台'
        ELSE platform
    END AS `平台`,
    seller_nick AS `店铺`,
    department_name AS `子账号分组`,
    snick AS `客服子账号`,
    employee_name AS `客服姓名`,
    superior_name AS `上级姓名`,
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
    sum(s_emotion_type_8_cnt) AS `客服骂人`
FROM (
    SELECT *
    FROM (
        SELECT *
        FROM (
            -- AI质检-子账号维度扣分质检项触发次数统计
            SELECT
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
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
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
                    -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                    -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                    SELECT distinct snick
                    FROM ods.xinghuan_employee_snick_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND platform = 'tb'
                    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            )
            AND abnormal_cnt!=0
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
        ) AS ai_abnormal_info
        GLOBAL FULL OUTER JOIN (
            -- AI质检-子账号维度加分质检项触发次数统计
            SELECT
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
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
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
                    -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                    -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                    SELECT distinct snick
                    FROM ods.xinghuan_employee_snick_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND platform = 'tb'
                    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            )
            AND excellent_cnt!=0
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
        ) AS ai_excellent_info
        USING(platform, seller_nick, snick)
    ) AS ai_abnormal_excellent_info
    GLOBAL FULL OUTER JOIN (
        SELECT *
        FROM (
            -- AI质检-子账号维度顾客情绪质检项触发次数统计
            SELECT
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
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
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
                    -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                    -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                    SELECT distinct snick
                    FROM ods.xinghuan_employee_snick_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                    AND platform = 'tb'
            )
            AND c_emotion_count!=0
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
        ) AS ai_c_emotion_info
        GLOBAL FULL OUTER JOIN(
            -- AI质检-子账号维度客服情绪质检项触发次数统计
            SELECT
                    platform,
                    seller_nick,
                    snick,
                    sumIf(s_emotion_count, s_emotion_type=8) AS s_emotion_type_8_cnt
            FROM dwd.xdqc_dialog_all
            ARRAY JOIN
                    s_emotion_type,
                    s_emotion_count
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
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
                    -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                    -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                    SELECT distinct snick
                    FROM ods.xinghuan_employee_snick_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                    AND platform = 'tb'
            )
            AND s_emotion_count!=0
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
        ) AS ai_s_emotion_info
        USING(platform, seller_nick, snick)
    ) AS ai_emotion_info
    USING(platform, seller_nick, snick)
) AS ai_check_info
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
GROUP BY platform, seller_nick, department_id, department_name, snick, employee_name, superior_name
HAVING department_id!='' -- 清除匹配不上历史分组的子账号
ORDER BY platform, seller_nick, department_name, snick, employee_name