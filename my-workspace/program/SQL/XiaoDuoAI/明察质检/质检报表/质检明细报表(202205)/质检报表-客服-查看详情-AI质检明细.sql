-- 质检报表-客服-查看详情-AI质检明细
-- 统计维度: 平台/店铺/子账号, 下钻维度路径: 平台/店铺/子账号分组/子账号/会话
SELECT
    dialog_id,
    day AS dialog_day,
    day AS `日期`,
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
    if(real_buyer_nick!='', real_buyer_nick, cnick) AS `顾客名称`,
    employee_name AS `客服姓名`,
    superior_name AS `上级姓名`,

    -- AI质检结果
    arrayStringConcat(ai_tag_names,'$$') AS `AI质检标签`,
    arrayStringConcat(ai_tag_cnts,'$$') AS `AI质检触发次数`
FROM (
    -- AI质检结果-会话维度质检项触发次数统计
    SELECT
        day,
        platform,
        seller_nick,
        snick,
        cnick,
        real_buyer_nick,
        dialog_id,
        arrayMap(x->toString(x), groupArray(tag_name)) AS ai_tag_names,
        arrayMap(x->toString(x), groupArray(tag_sum)) AS ai_tag_cnts
    FROM (
        SELECT
            day,
            platform,
            seller_nick,
            snick,
            cnick,
            real_buyer_nick,
            dialog_id,
            tag_type,
            tag_id,
            SUM(tag_cnt) AS tag_sum
        FROM (
            -- AI质检项
            SELECT
                toYYYYMMDD(begin_time) AS day,
                platform,
                seller_nick,
                snick,
                cnick,
                real_buyer_nick,
                _id AS dialog_id,
                
                arrayConcat(
                    -- 旧版本AI质检项-非情绪扣分项-质检类型
                    arrayResize(['ai_abnormal'], length(abnormals_type), 'ai_abnormal'),
                    -- 旧版本AI质检项-非情绪加分项-质检类型
                    arrayResize(['ai_excellent'], length(excellents_type), 'ai_excellent'),
                    -- 旧版本AI质检项-买家情绪项-质检类型
                    arrayResize(['ai_c_emotion'], length(c_emotion_type), 'ai_c_emotion'),
                    -- 旧版本AI质检项-客服情绪项-质检类型
                    arrayResize(['ai_s_emotion'], length(s_emotion_type), 'ai_s_emotion')
                ) AS tag_types,
                arrayConcat(
                    -- 旧版本AI质检项-非情绪扣分项-质检项ID
                    arrayMap((x)->toString(x), abnormals_type),
                    -- 旧版本AI质检项-非情绪加分项-质检项ID
                    arrayMap((x)->toString(x), excellents_type),
                    -- 旧版本AI质检项-买家情绪项-质检项ID
                    arrayMap((x)->toString(x), c_emotion_type),
                    -- 旧版本AI质检项-非情绪扣分项-质检项ID
                    arrayMap((x)->toString(x), s_emotion_type)
                ) AS tag_ids,
                arrayConcat(
                    -- 旧版本AI质检项-非情绪扣分项-质检项触发次数
                    abnormals_count,
                    -- 旧版本AI质检项-非情绪加分项-质检项触发次数
                    excellents_count,
                    -- 旧版本AI质检项-买家情绪项-质检项触发次数
                    c_emotion_count,
                    -- 旧版本AI质检项-非情绪扣分项-质检项触发次数
                    s_emotion_count
                ) AS tag_cnts
            FROM dwd.xdqc_dialog_all
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start_=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end_=yesterday }}'))
            AND platform = 'tb'
            AND seller_nick GLOBAL IN (
                -- 查询对应企业-平台的店铺
                SELECT DISTINCT seller_nick
                FROM xqc_dim.xqc_shop_all
                WHERE day=toYYYYMMDD(yesterday())
                AND platform = 'tb'
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            )
            -- 下拉框-店铺名
            AND (
                    '{{ seller_nicks_ }}'=''
                    OR
                    seller_nick IN splitByChar(',','{{ seller_nicks_ }}')
            )
            -- 筛选AI质检已打标数据
            AND arraySum(tag_cnts) != 0

        ) AS ods_ai_tag
        ARRAY JOIN
            tag_types AS tag_type,
            tag_ids AS tag_id,
            tag_cnts AS tag_cnt,
            tag_mds AS tag_md
        WHERE snick GLOBAL IN (
            -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
            -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        )
        -- 下拉框-子账号
        AND (
            '{{ snicks_ }}'=''
            OR
            snick IN splitByChar(',','{{ snicks_ }}')
        )
        -- 过滤空数据
        AND tag_cnt!=0
        GROUP BY day, platform, seller_nick, snick, cnick, real_buyer_nick, dialog_id, tag_type, tag_id
    ) AS ods_ai_tag
    GLOBAL LEFT JOIN (
        -- 关联AI质检项
        SELECT
            qc_rule_type AS tag_type,
            qc_rule_id AS tag_id,
            qc_rule_name AS tag_name
        FROM xqc_dim.qc_rule_constant_all
        WHERE day=toYYYYMMDD(yesterday())
    ) AS dim_tag
    USING(tag_type, tag_id)
    GROUP BY day, platform, seller_nick, snick, cnick, real_buyer_nick, dialog_id
) AS ods_ai_tag_stat
GLOBAL LEFT JOIN (
    -- 获取子账号完整信息
    SELECT
        snick, employee_name, superior_name, department_id, department_name
    FROM xqc_dim.snick_full_info_all
    WHERE day = toYYYYMMDD(yesterday())
    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
) AS dim_snick_department
USING(snick)
-- 下拉框-客服姓名
WHERE (
    '{{ usernames }}'=''
    OR
    employee_name IN splitByChar(',','{{ usernames }}')
)
ORDER BY day ASC