-- 客户评价满意度-分析-质检项触发次数分布-AI质检
SELECT
    tag_name AS `质检标签`,
    sum(tag_cnt_sum) AS `触发次数`
FROM (
    -- AI质检-子账号维度扣分质检项触发次数统计
    SELECT
        platform,
        seller_nick,
        snick,
        tag_id,
        SUM(tag_cnt) AS tag_cnt_sum
    FROM (
        SELECT
            platform,
            seller_nick,
            snick,
            if(order_info_status[1]='','unorder',order_info_status[1]) AS order_status,
            arrayConcat(abnormals_rule_id, excellents_rule_id, c_emotion_rule_id, s_emotion_rule_id) AS tag_ids,
            arrayConcat(abnormals_count, excellents_count, c_emotion_count, s_emotion_count) AS tag_cnts
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
        AND channel = 'tb' AND platform = 'tb'
        -- 查询当前企业对应店铺
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
        AND (toYYYYMMDD(begin_time), snick, cnick) GLOBAL IN (
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
                WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
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
                AND user_nick GLOBAL IN (
                    SELECT plat_snick
                    FROM (
                        -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                        SELECT distinct CONCAT('cntaobao', snick) AS plat_snick
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = toYYYYMMDD(yesterday())
                        AND platform = 'tb'
                        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
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
        tag_ids AS tag_id, 
        tag_cnts AS tag_cnt
    WHERE tag_id!='' AND tag_cnt!=0
    GROUP BY platform, seller_nick, snick, tag_id
) AS tag_stat_info
GLOBAL LEFT JOIN (
    -- 查询AI质检项
    SELECT
        _id AS tag_id,
        name AS tag_name
    FROM xqc_dim.qc_rule_all
    WHERE day = toYYYYMMDD(yesterday())
    AND rule_category = 1
) AS dim_tag
USING(tag_id)
GROUP BY tag_name
ORDER BY `触发次数` DESC