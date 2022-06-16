-- 客户评价满意度-分析-质检项触发次数分布-AI质检
SELECT
    tag_name AS `质检标签`,
    sum(tag_cnt) AS `触发次数`
FROM (
    -- AI质检-子账号维度扣分质检项触发次数统计
    SELECT
        platform,
        seller_nick,
        snick,
        abnormal_type AS tag_id,
        CASE
            WHEN abnormal_type=1 THEN '非客服结束会话'
            WHEN abnormal_type=2 THEN '漏跟进'
            WHEN abnormal_type=3 THEN '快捷短语重复'
            WHEN abnormal_type=4 THEN '生硬拒绝'
            WHEN abnormal_type=5 THEN '欠缺安抚'
            WHEN abnormal_type=6 THEN '答非所问'
            WHEN abnormal_type=7 THEN '单字回复'
            WHEN abnormal_type=8 THEN '单句响应慢'
            WHEN abnormal_type=9 THEN '产品不熟悉'
            WHEN abnormal_type=10 THEN '活动不熟悉'
            WHEN abnormal_type=11 THEN '内部回复慢'
            WHEN abnormal_type=12 THEN '回复严重超时'
            WHEN abnormal_type=13 THEN '撤回人工消息'
            WHEN abnormal_type=14 THEN '单表情回复'
            WHEN abnormal_type=15 THEN '异常撤回'
            WHEN abnormal_type=16 THEN '转接前未有效回复'
            WHEN abnormal_type=17 THEN '超时未回复'
            WHEN abnormal_type=18 THEN '顾客撤回'
            WHEN abnormal_type=19 THEN '前后回复矛盾'
            WHEN abnormal_type=20 THEN '撤回机器人消息'
            WHEN abnormal_type=21 THEN '第三方投诉或曝光'
            WHEN abnormal_type=22 THEN '顾客提及投诉或举报'
            WHEN abnormal_type=23 THEN '差评或要挟差评'
            WHEN abnormal_type=24 THEN '反问/质疑顾客'
            WHEN abnormal_type=25 THEN '违禁词'
            WHEN abnormal_type=26 THEN '客服冷漠讥讽'
            WHEN abnormal_type=27 THEN '顾客怀疑假货'
            WHEN abnormal_type=28 THEN '客服态度消极敷衍'
            WHEN abnormal_type=29 THEN '售后不满意'
            ELSE '其他'
        END AS tag_name,
        sum(abnormal_cnt) AS tag_cnt
    FROM (
        SELECT
            platform,
            seller_nick,
            snick,
            if(order_info_status[1]='','unorder',order_info_status[1]) AS order_status,
            abnormals_type,
            abnormals_count
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_}}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
        AND channel = 'tb' AND platform = 'tb'
        AND seller_nick GLOBAL IN (
            SELECT DISTINCT seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day = toYYYYMMDD(yesterday())
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
            -- 下拉框-子账号分组
            AND (
                '{{ department_ids }}'=''
                OR
                department_id IN splitByChar(',','{{ department_ids }}')
            )
            -- 下拉框-子账号
            AND (
                '{{ snicks }}'=''
                OR
                snick IN splitByChar(',','{{ snicks }}')
            )
            -- 下拉框-客服姓名ID
            AND (
                '{{ employee_ids }}'=''
                OR
                employee_id IN splitByChar(',','{{ employee_ids }}')
            )
        )
        AND (toYYYYMMDD(begin_time),snick,cnick) GLOBAL IN (
                -- 查询已有评价的子账号
                SELECT DISTINCT
                    toUInt32(day), snick, cnick
                FROM (
                    SELECT
                        replaceOne(splitByChar(':', user_nick)[1], 'cntaobao', '') AS seller_nick,
                        replaceOne(user_nick, 'cntaobao', '') AS snick,
                        replaceOne(eval_recer,'cntaobao','') AS cnick,
                        eval_code,
                        day
                    FROM ods.kefu_eval_detail_all
                    WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=week_ago}}')) AND toYYYYMMDD(toDate('{{day.end=yesterday}}'))
                    -- 过滤买家已评价记录
                    AND eval_time != ''
                    -- 下拉框-评价等级
                    AND (
                        '{{ eval_codes }}'=''
                        OR
                        toString(eval_code) IN splitByChar(',','{{ eval_codes }}')
                    )
                    -- 下拉框-店铺名
                    AND (
                        '{{ seller_nicks }}'=''
                        OR
                        seller_nick IN splitByChar(',','{{ seller_nicks }}')
                    )
                    AND snick IN (
                        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
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
                        -- 下拉框-子账号
                        AND (
                            '{{ snicks }}'=''
                            OR
                            snick IN splitByChar(',','{{ snicks }}')
                        )
                        -- 下拉框-客服姓名ID
                        AND (
                            '{{ employee_ids }}'=''
                            OR
                            employee_id IN splitByChar(',','{{ employee_ids }}')
                        )
                    )
                ) AS eval_info
        )
        -- 下拉框-店铺名
        AND (
            '{{ seller_nicks }}'=''
            OR
            seller_nick IN splitByChar(',','{{ seller_nicks }}')
        )
        -- 下拉框-订单状态
        AND (
            '{{ order_statuses }}'=''
            OR
            order_status IN splitByChar(',','{{ order_statuses }}')
        )
    ) AS dialog_ai_abnormal_info
    ARRAY JOIN
        abnormals_type AS abnormal_type, 
        abnormals_count AS abnormal_cnt
    WHERE abnormal_cnt!=0
    GROUP BY platform, seller_nick, snick, abnormal_type
    
    UNION ALL
    -- AI质检-子账号维度加分质检项触发次数统计
    SELECT
        platform,
        seller_nick,
        snick,
        excellent_type AS tag_id,
        CASE
            WHEN excellent_type=1 THEN '需求挖掘'
            WHEN excellent_type=2 THEN '商品细节解答'
            WHEN excellent_type=3 THEN '卖点传达'
            WHEN excellent_type=4 THEN '商品推荐'
            WHEN excellent_type=5 THEN '退换货理由修改'
            WHEN excellent_type=6 THEN '主动跟进'
            WHEN excellent_type=7 THEN '无货挽回'
            WHEN excellent_type=8 THEN '活动传达'
            WHEN excellent_type=9 THEN '店铺保障'
            WHEN excellent_type=10 THEN '催拍催付'
            WHEN excellent_type=11 THEN '核对地址'
            WHEN excellent_type=12 THEN '好评引导'
            WHEN excellent_type=13 THEN '优秀结束语'
            ELSE '其他'
        END AS tag_name,
        sum(excellent_cnt) AS tag_cnt
    FROM (
        SELECT
            platform,
            seller_nick,
            snick,
            if(order_info_status[1]='','unorder',order_info_status[1]) AS order_status,
            excellents_type,
            excellents_count
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_}}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
        AND channel = 'tb' AND platform = 'tb'
        AND seller_nick GLOBAL IN (
            SELECT DISTINCT seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day = toYYYYMMDD(yesterday())
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
            -- 下拉框-子账号分组
            AND (
                '{{ department_ids }}'=''
                OR
                department_id IN splitByChar(',','{{ department_ids }}')
            )
            -- 下拉框-子账号
            AND (
                '{{ snicks }}'=''
                OR
                snick IN splitByChar(',','{{ snicks }}')
            )
            -- 下拉框-客服姓名ID
            AND (
                '{{ employee_ids }}'=''
                OR
                employee_id IN splitByChar(',','{{ employee_ids }}')
            )
        )
        AND (toYYYYMMDD(begin_time),snick,cnick) GLOBAL IN (
                -- 查询已有评价的子账号
                SELECT DISTINCT
                    toUInt32(day), snick, cnick
                FROM (
                    SELECT
                        replaceOne(splitByChar(':', user_nick)[1], 'cntaobao', '') AS seller_nick,
                        replaceOne(user_nick, 'cntaobao', '') AS snick,
                        replaceOne(eval_recer,'cntaobao','') AS cnick,
                        eval_code,
                        day
                    FROM ods.kefu_eval_detail_all
                    WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=week_ago}}')) AND toYYYYMMDD(toDate('{{day.end=yesterday}}'))
                    -- 过滤买家已评价记录
                    AND eval_time != ''
                    -- 下拉框-评价等级
                    AND (
                        '{{ eval_codes }}'=''
                        OR
                        toString(eval_code) IN splitByChar(',','{{ eval_codes }}')
                    )
                    -- 下拉框-店铺名
                    AND (
                        '{{ seller_nicks }}'=''
                        OR
                        seller_nick IN splitByChar(',','{{ seller_nicks }}')
                    )
                    AND snick IN (
                        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
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
                        -- 下拉框-子账号
                        AND (
                            '{{ snicks }}'=''
                            OR
                            snick IN splitByChar(',','{{ snicks }}')
                        )
                        -- 下拉框-客服姓名ID
                        AND (
                            '{{ employee_ids }}'=''
                            OR
                            employee_id IN splitByChar(',','{{ employee_ids }}')
                        )
                    )
                ) AS eval_info
        )
        -- 下拉框-店铺名
        AND (
            '{{ seller_nicks }}'=''
            OR
            seller_nick IN splitByChar(',','{{ seller_nicks }}')
        )
        -- 下拉框-订单状态
        AND (
            '{{ order_statuses }}'=''
            OR
            order_status IN splitByChar(',','{{ order_statuses }}')
        )
    ) AS dialog_ai_excellent_info
    ARRAY JOIN
        excellents_type AS excellent_type, 
        excellents_count AS excellent_cnt
    WHERE excellent_cnt!=0
    GROUP BY platform, seller_nick, snick, excellent_type

    UNION ALL
    -- AI质检-子账号维度顾客情绪质检项触发次数统计
    SELECT
        platform,
        seller_nick,
        snick,
        c_emotion_type AS tag_id,
        CASE
            WHEN c_emotion_type=1 THEN '满意'
            WHEN c_emotion_type=2 THEN '感激'
            WHEN c_emotion_type=3 THEN '期待'
            WHEN c_emotion_type=4 THEN '对客服态度不满'
            WHEN c_emotion_type=5 THEN '对发货物流不满'
            WHEN c_emotion_type=6 THEN '对产品不满'
            WHEN c_emotion_type=7 THEN '其他不满意'
            WHEN c_emotion_type=8 THEN '顾客骂人'
            WHEN c_emotion_type=9 THEN '对收货少件不满'
            ELSE '其他'
        END AS tag_name,
        sum(c_emotion_count) AS tag_cnt
    FROM (
        SELECT
            platform,
            seller_nick,
            snick,
            if(order_info_status[1]='','unorder',order_info_status[1]) AS order_status,
            c_emotion_type,
            c_emotion_count
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_}}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
        AND channel = 'tb' AND platform = 'tb'
        AND seller_nick GLOBAL IN (
            SELECT DISTINCT seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day = toYYYYMMDD(yesterday())
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
            -- 下拉框-子账号分组
            AND (
                '{{ department_ids }}'=''
                OR
                department_id IN splitByChar(',','{{ department_ids }}')
            )
            -- 下拉框-子账号
            AND (
                '{{ snicks }}'=''
                OR
                snick IN splitByChar(',','{{ snicks }}')
            )
            -- 下拉框-客服姓名ID
            AND (
                '{{ employee_ids }}'=''
                OR
                employee_id IN splitByChar(',','{{ employee_ids }}')
            )
        )
        AND (toYYYYMMDD(begin_time),snick,cnick) GLOBAL IN (
                -- 查询已有评价的子账号
                SELECT DISTINCT
                    toUInt32(day), snick, cnick
                FROM (
                    SELECT
                        replaceOne(splitByChar(':', user_nick)[1], 'cntaobao', '') AS seller_nick,
                        replaceOne(user_nick, 'cntaobao', '') AS snick,
                        replaceOne(eval_recer,'cntaobao','') AS cnick,
                        eval_code,
                        day
                    FROM ods.kefu_eval_detail_all
                    WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=week_ago}}')) AND toYYYYMMDD(toDate('{{day.end=yesterday}}'))
                    -- 过滤买家已评价记录
                    AND eval_time != ''
                    -- 下拉框-评价等级
                    AND (
                        '{{ eval_codes }}'=''
                        OR
                        toString(eval_code) IN splitByChar(',','{{ eval_codes }}')
                    )
                    -- 下拉框-店铺名
                    AND (
                        '{{ seller_nicks }}'=''
                        OR
                        seller_nick IN splitByChar(',','{{ seller_nicks }}')
                    )
                    AND snick IN (
                        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
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
                        -- 下拉框-子账号
                        AND (
                            '{{ snicks }}'=''
                            OR
                            snick IN splitByChar(',','{{ snicks }}')
                        )
                        -- 下拉框-客服姓名ID
                        AND (
                            '{{ employee_ids }}'=''
                            OR
                            employee_id IN splitByChar(',','{{ employee_ids }}')
                        )
                    )
                ) AS eval_info
        )
        -- 下拉框-店铺名
        AND (
            '{{ seller_nicks }}'=''
            OR
            seller_nick IN splitByChar(',','{{ seller_nicks }}')
        )
        -- 下拉框-订单状态
        AND (
            '{{ order_statuses }}'=''
            OR
            order_status IN splitByChar(',','{{ order_statuses }}')
        )
    ) AS dialog_ai_c_emotion_info
    ARRAY JOIN
        c_emotion_type,
        c_emotion_count
    WHERE c_emotion_count!=0
    GROUP BY platform, seller_nick, snick, c_emotion_type

    UNION ALL
    -- AI质检-子账号维度客服情绪质检项触发次数统计
    SELECT
        platform,
        seller_nick,
        snick,
        s_emotion_type AS tag_id,
        CASE
            WHEN s_emotion_type=8 THEN '客服骂人'
            ELSE '其他'
        END AS tag_name,
        sum(s_emotion_count) AS tag_cnt
    FROM (
        SELECT
            platform,
            seller_nick,
            snick,
            if(order_info_status[1]='','unorder',order_info_status[1]) AS order_status,
            s_emotion_type,
            s_emotion_count
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_}}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
        AND channel = 'tb' AND platform = 'tb'
        AND seller_nick GLOBAL IN (
            SELECT DISTINCT seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day = toYYYYMMDD(yesterday())
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
            -- 下拉框-子账号分组
            AND (
                '{{ department_ids }}'=''
                OR
                department_id IN splitByChar(',','{{ department_ids }}')
            )
            -- 下拉框-子账号
            AND (
                '{{ snicks }}'=''
                OR
                snick IN splitByChar(',','{{ snicks }}')
            )
            -- 下拉框-客服姓名ID
            AND (
                '{{ employee_ids }}'=''
                OR
                employee_id IN splitByChar(',','{{ employee_ids }}')
            )
        )
        AND (toYYYYMMDD(begin_time),snick,cnick) GLOBAL IN (
                -- 查询已有评价的子账号
                SELECT DISTINCT
                    toUInt32(day), snick, cnick
                FROM (
                    SELECT
                        replaceOne(splitByChar(':', user_nick)[1], 'cntaobao', '') AS seller_nick,
                        replaceOne(user_nick, 'cntaobao', '') AS snick,
                        replaceOne(eval_recer,'cntaobao','') AS cnick,
                        eval_code,
                        day
                    FROM ods.kefu_eval_detail_all
                    WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=week_ago}}')) AND toYYYYMMDD(toDate('{{day.end=yesterday}}'))
                    -- 过滤买家已评价记录
                    AND eval_time != ''
                    -- 下拉框-评价等级
                    AND (
                        '{{ eval_codes }}'=''
                        OR
                        toString(eval_code) IN splitByChar(',','{{ eval_codes }}')
                    )
                    -- 下拉框-店铺名
                    AND (
                        '{{ seller_nicks }}'=''
                        OR
                        seller_nick IN splitByChar(',','{{ seller_nicks }}')
                    )
                    AND snick IN (
                        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
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
                        -- 下拉框-子账号
                        AND (
                            '{{ snicks }}'=''
                            OR
                            snick IN splitByChar(',','{{ snicks }}')
                        )
                        -- 下拉框-客服姓名ID
                        AND (
                            '{{ employee_ids }}'=''
                            OR
                            employee_id IN splitByChar(',','{{ employee_ids }}')
                        )
                    )
                ) AS eval_info
        )
        -- 下拉框-店铺名
        AND (
            '{{ seller_nicks }}'=''
            OR
            seller_nick IN splitByChar(',','{{ seller_nicks }}')
        )
        -- 下拉框-订单状态
        AND (
            '{{ order_statuses }}'=''
            OR
            order_status IN splitByChar(',','{{ order_statuses }}')
        )
    ) AS dialog_ai_s_emotion_info
    ARRAY JOIN
        s_emotion_type,
        s_emotion_count
    WHERE s_emotion_count!=0
    GROUP BY platform, seller_nick, snick, s_emotion_type
) AS tag_stat_info
GROUP BY tag_name
ORDER BY `触发次数` DESC