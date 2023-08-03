-- 质检报表-客服
-- 统计维度: 平台/店铺/子账号分组/子账号-客服
-- 下钻维度路径: 平台/店铺/子账号分组/子账号-客服/会话

SELECT
    platform AS `平台`,
    seller_nick AS `店铺`, department_name AS `子账号分组`, snick AS `客服子账号`,
    employee_name AS `客服姓名`, superior_name AS `客服上级姓名`,

    dialog_cnt AS `总会话量`,
    round((dialog_cnt*100 + add_score_sum- subtract_score_sum)/dialog_cnt,2) AS `平均分`,

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
            -- 会话量统计-总计
            sum(dialog_cnt) AS dialog_cnt
        FROM xqc_dws.snick_stat_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start }}')) AND toYYYYMMDD(toDate('{{ day.end }}'))
        AND platform = '{{platform}}'
        AND seller_nick GLOBAL IN (
            -- 查询对应企业-平台的店铺
            SELECT DISTINCT seller_nick
            FROM xqc_dim.shop_latest_all
            WHERE platform = '{{platform}}'
            AND company_id = '{{ company_id }}'
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
                platform, seller_nick, snick, 
                tag_type, tag_id, tag_name,
                sum(tag_cnt_sum) AS tag_cnt_sum,
                sum(tag_score_sum) AS tag_score_sum
            FROM xqc_dws.tag_stat_all
            WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start }}')) AND toYYYYMMDD(toDate('{{ day.end }}'))
            AND platform = '{{platform}}'
            AND seller_nick GLOBAL IN (
                -- 查询对应企业-平台的店铺
                SELECT DISTINCT seller_nick
                FROM xqc_dim.shop_latest_all
                WHERE platform = '{{platform}}'
                AND company_id = '{{ company_id }}'
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
        platform, snick, employee_name, superior_name, department_id, department_name
    FROM xqc_dim.snick_full_info_all
    WHERE day = toYYYYMMDD(yesterday())
    AND platform = '{{platform}}'
    AND company_id = '{{ company_id }}'
) AS dim_snick_department
USING(platform, snick)