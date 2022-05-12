-- 客户评价满意度-分析-质检项触发次数分布-自定义质检
SELECT
    tag_name AS `质检标签`,
    sum(tag_cnt) AS `触发次数`
FROM (
    -- 自定义质检结果-子账号维度
    SELECT
        platform,
        seller_nick,
        snick,
        tag_name,
        sum(tag_cnt) AS tag_cnt
    FROM (
        -- 自定义质检-标签维度-扣分质检项触发次数统计
        SELECT
            platform,
            seller_nick,
            snick,
            tag_id,
            sum(tag_count) AS tag_cnt
        FROM (
            SELECT
                platform,
                seller_nick,
                snick,
                if(order_info_status[1]='','unorder',order_info_status[1]) AS order_status,
                rule_stats_id,
                rule_stats_count
            FROM dwd.xdqc_dialog_all
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
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
                        replaceOne(splitByChar(':',user_nick)[1],'cntaobao','') AS seller_nick,
                        replaceOne(eval_sender,'cntaobao','') AS snick,
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
        ) AS dialog_rule_stats_info
        ARRAY JOIN
            rule_stats_id AS tag_id,
            rule_stats_count AS tag_count
        -- 清除没有打标的数据, 减小计算量
        WHERE rule_stats_id!=[]
        GROUP BY platform, seller_nick, snick, tag_id
        
        UNION ALL
        -- 自定义质检-标签维度-加分质检项触发次数统计
        SELECT
            platform,
            seller_nick,
            snick,
            tag_id,
            sum(tag_count) AS tag_cnt
        FROM (
            SELECT
                platform,
                seller_nick,
                snick,
                if(order_info_status[1]='','unorder',order_info_status[1]) AS order_status,
                rule_add_stats_id,
                rule_add_stats_count
            FROM dwd.xdqc_dialog_all
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
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
                        replaceOne(splitByChar(':',user_nick)[1],'cntaobao','') AS seller_nick,
                        replaceOne(eval_sender,'cntaobao','') AS snick,
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
        ) AS dialog_rule_add_stats_info
        ARRAY JOIN
            rule_add_stats_id AS tag_id,
            rule_add_stats_count AS tag_count
        -- 清除没有打标的数据, 减小计算量
        WHERE rule_add_stats_id!=[]
        GROUP BY platform, seller_nick, snick, tag_id

        UNION ALL
        -- 自定义质检-标签维度-消息质检项触发次数统计
        SELECT
            platform,
            seller_nick,
            snick,
            tag_id,
            sum(tag_count) AS tag_cnt
        FROM (
            SELECT
                platform,
                seller_nick,
                snick,
                if(order_info_status[1]='','unorder',order_info_status[1]) AS order_status,
                xrule_stats_id,
                xrule_stats_count
            FROM dwd.xdqc_dialog_all
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
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
                        replaceOne(splitByChar(':',user_nick)[1],'cntaobao','') AS seller_nick,
                        replaceOne(eval_sender,'cntaobao','') AS snick,
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
        ) AS dialog_rule_stats_info
        ARRAY JOIN
            xrule_stats_id AS tag_id,
            xrule_stats_count AS tag_count
        -- 清除没有打标的数据, 减小计算量
        WHERE xrule_stats_id!=[]
        GROUP BY platform, seller_nick, snick, tag_id

        UNION ALL
        -- 自定义质检-标签维度-会话质检项触发次数统计
        SELECT
            platform,
            seller_nick,
            snick,
            tag_id,
            sum(tag_count) AS tag_cnt
        FROM (
            SELECT
                platform,
                seller_nick,
                snick,
                if(order_info_status[1]='','unorder',order_info_status[1]) AS order_status,
                top_xrules_id,
                top_xrules_count
            FROM dwd.xdqc_dialog_all
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
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
                        replaceOne(splitByChar(':',user_nick)[1],'cntaobao','') AS seller_nick,
                        replaceOne(eval_sender,'cntaobao','') AS snick,
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
        ) AS dialog_rule_stats_info
        ARRAY JOIN
            top_xrules_id AS tag_id,
            top_xrules_count AS tag_count
        -- 清除没有打标的数据, 减小计算量
        WHERE top_xrules_id!=[]
        GROUP BY platform, seller_nick, snick, tag_id

    ) AS customize_check_stat
    GLOBAL LEFT JOIN (
        -- 获取自定义质检项
        SELECT
            _id AS tag_id,
            name AS tag_name
        FROM xqc_dim.qc_rule_all
        WHERE day = toYYYYMMDD(yesterday())
        AND rule_category = 3
        AND platform = 'tb'
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    ) AS customize_tag_info
    USING(tag_id)
    GROUP BY platform, seller_nick, snick, tag_name
) AS tag_stat_info
GROUP BY tag_name
ORDER BY `触发次数` DESC