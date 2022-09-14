-- 客户评价满意度-分析-质检项触发次数分布-AI质检
SELECT
    tag_name,
    sum(tag_sum) AS tag_cnt_sum,
    tag_name AS `质检标签`,
    tag_cnt_sum AS `触发次数`
FROM (
    -- AI质检-子账号维度扣分质检项触发次数统计
    SELECT
        platform,
        seller_nick,
        snick,
        tag_id,
        sum(tag_cnt) AS tag_sum
    FROM (
        SELECT
            platform,
            seller_nick,
            snick,
            if(order_info_status[1]='','unorder',order_info_status[1]) AS order_status,
            -- AI质检项ID
            arrayConcat(
                -- AI质检项-扣分项
                arrayFilter(x->x!='', abnormals_rule_id),
                -- AI质检项-加分项
                arrayFilter(x->x!='', excellents_rule_id),
                -- AI质检项-买家情绪项
                arrayFilter(x->x!='', c_emotion_rule_id),
                -- AI质检项-客服情绪项
                arrayFilter(x->x!='', s_emotion_rule_id)
            ) AS tag_ids,
            -- AI质检项触发次数
            arrayConcat(
                -- AI质检项-扣分项
                arrayFilter(x->x!=0, abnormals_count),
                -- AI质检项-加分项
                arrayFilter(x->x!=0, excellents_count),
                -- AI质检项-买家情绪项
                arrayFilter(x->x!=0, c_emotion_count),
                -- AI质检项-客服情绪项
                arrayFilter(x->x!=0, s_emotion_count)
            ) AS tag_cnts
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=yesterday }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
        AND platform = 'tb'
        -- 下拉框-订单状态
        AND (
            '{{ order_statuses }}'=''
            OR
            order_status IN splitByChar(',','{{ order_statuses }}')
        )
        -- 筛选指定主账号
        AND seller_nick GLOBAL IN (
            SELECT DISTINCT
                seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            -- 下拉框-主账号
            AND (
                '{{ seller_nicks }}'=''
                OR
                seller_nick IN splitByChar(',','{{ seller_nicks }}')
            )
        )
        -- 筛选指定子账号
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
        -- 筛选有评价的dialog_id
        AND _id GLOBAL IN (
            SELECT DISTINCT
                dialog_id
            FROM xqc_ods.dialog_eval_all
            WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=yesterday }}'))
                AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
            AND platform = 'tb'
            -- 下拉框-主账号
            AND (
                '{{ seller_nicks }}'=''
                OR
                seller_nick IN splitByChar(',','{{ seller_nicks }}')
            )
            -- 筛选指定子账号
            AND snick GLOBAL IN (
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
                -- 下拉框-客服姓名ID
                AND (
                    '{{ employee_ids }}'=''
                    OR
                    employee_id IN splitByChar(',','{{ employee_ids }}')
                )
                -- 下拉框-子账号
                AND (
                    '{{ snicks }}'=''
                    OR
                    snick IN splitByChar(',','{{ snicks }}')
                )
            )
            -- 过滤买家已评价记录
            AND eval_time != ''
            -- 下拉框-评价等级
            AND (
                '{{ eval_codes }}'=''
                OR
                toString(eval_code) IN splitByChar(',','{{ eval_codes }}')
            )
        ) AS eval_info
    ) AS dialog_ai_tag_detail
    ARRAY JOIN
        tag_ids AS tag_id,
        tag_cnts AS tag_cnt
    WHERE tag_cnt!=0
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
    -- 筛选企业对应质检标准
    AND qc_norm_id GLOBAL IN (
        SELECT DISTINCT
            _id AS qc_norm_id
        FROM ods.xinghuan_qc_norm_all
        WHERE day = toYYYYMMDD(yesterday())
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        AND platform = 'tb'
    )
) AS tag_dim_info
USING(tag_id)
GROUP BY tag_id, tag_name
ORDER BY tag_cnt_sum DESC