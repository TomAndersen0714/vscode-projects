        SELECT
            day, platform, seller_nick, snick,
            dim_snick_department.employee_id,
            dim_snick_department.employee_name,
            dim_snick_department.department_id,
            dim_snick_department.department_name,
            -- 分值统计-总计
            subtract_score_sum,
            add_score_sum,
            -- 分值统计-AI质检
            ai_subtract_score_sum,
            ai_add_score_sum,
            -- 分值统计-自定义质检
            custom_subtract_score_sum,
            custom_add_score_sum,
            -- 分值统计-人工质检
            manual_subtract_score_sum,
            manual_add_score_sum,
            -- 会话量统计-总计
            dialog_cnt,
            manual_marked_dialog_cnt, -- 被人工质检会话量
            -- 会话量统计-AI质检
            ai_subtract_score_dialog_cnt,
            ai_add_score_dialog_cnt,
            -- abnormal_dialog_cnt,
            -- excellent_dialog_cnt,
            -- c_emotion_dialog_cnt,
            -- s_emotion_dialog_cnt,
            -- 会话量统计-自定义质检
            custom_subtract_score_dialog_cnt,
            custom_add_score_dialog_cnt,
            -- 会话量统计-人工质检
            manual_subtract_score_dialog_cnt, -- 人工质检扣分会话量
            manual_add_score_dialog_cnt -- 人工质检加分会话量
        FROM (
            SELECT
                toYYYYMMDD(begin_time) AS day,
                platform,
                seller_nick,
                snick,
                -- 分值统计-总计
                sum(score) AS subtract_score_sum,
                sum(score_add) AS add_score_sum,
                -- 分值统计-人工质检
                sum(mark_score) AS manual_subtract_score_sum,
                sum(mark_score_add) AS manual_add_score_sum,
                -- 分值统计-自定义质检
                sum(
                    arraySum(rule_stats_score)
                    +
                    negate(arraySum(arrayMap((x,y) -> x*if(y<0,y,0), xrule_stats_count, xrule_stats_score)))
                    +
                    negate(arraySum(arrayMap((x,y) -> x*if(y<0,y,0), top_xrules_count, top_xrules_score)))
                ) AS custom_subtract_score_sum,
                sum(
                    arraySum(rule_add_stats_score)
                    +
                    arraySum(arrayMap((x,y) -> x*if(y>0,y,0), xrule_stats_count, xrule_stats_score))
                    +
                    arraySum(arrayMap((x,y) -> x*if(y>0,y,0), top_xrules_count, top_xrules_score))
                ) AS custom_add_score_sum,
                -- 分值统计-AI质检
                subtract_score_sum - manual_subtract_score_sum - custom_subtract_score_sum AS ai_subtract_score_sum,
                add_score_sum - manual_add_score_sum - custom_add_score_sum AS ai_add_score_sum,
        
                -- 会话量统计-总计
                COUNT(1) AS dialog_cnt,
                -- 会话量统计-AI质检
                sum((
                    score - mark_score - (
                        arraySum(rule_stats_score)
                        +
                        negate(arraySum(arrayMap((x,y) -> x*if(y<0,y,0), xrule_stats_count, xrule_stats_score)))
                        +
                        negate(arraySum(arrayMap((x,y) -> x*if(y<0,y,0), top_xrules_count, top_xrules_score)))
                    )) > 0
                ) AS ai_subtract_score_dialog_cnt,
                sum((
                    score_add - mark_score_add - (
                        arraySum(rule_add_stats_score)
                        +
                        arraySum(arrayMap((x,y) -> x*if(y>0,y,0), xrule_stats_count, xrule_stats_score))
                        +
                        arraySum(arrayMap((x,y) -> x*if(y>0,y,0), top_xrules_count, top_xrules_score))
                    )) > 0
                ) AS ai_add_score_dialog_cnt,
                -- sum(arraySum(abnormals_count)!=0) AS abnormal_dialog_cnt,
                -- sum(arraySum(excellents_count)!=0) AS excellent_dialog_cnt,
                -- sum(arraySum(c_emotion_count)!=0) AS c_emotion_dialog_cnt,
                -- sum(arraySum(s_emotion_count)!=0) AS s_emotion_dialog_cnt,
                -- 会话量统计-自定义质检
                sum((
                        length(rule_stats_id)
                        +
                        length(arrayFilter(x->x<0, xrule_stats_score))
                        +
                        length(arrayFilter(x->x<0, top_xrules_score))
                    )!=0
                ) AS custom_subtract_score_dialog_cnt,
                sum((
                        length(rule_add_stats_id)
                        +
                        length(arrayFilter(x->x>0, xrule_stats_score))
                        +
                        length(arrayFilter(x->x>0, top_xrules_score))
                    )!=0
                ) AS custom_add_score_dialog_cnt,
                -- 会话量统计-人工质检
                sum(length(mark_ids)!=0) AS manual_marked_dialog_cnt, -- 被人工质检会话量
                sum(arrayExists((x)->x>0, tag_score_stats_score)) AS manual_subtract_score_dialog_cnt, -- 人工质检扣分会话量
                sum(arrayExists((x)->x>0, tag_score_add_stats_score)) AS manual_add_score_dialog_cnt -- 人工质检加分会话量
            FROM dwd.xdqc_dialog_all
            WHERE toYYYYMMDD(begin_time) = {ds_nodash}
            GROUP BY day, platform, seller_nick, snick
        ) AS dws_snick_stat
        GLOBAL LEFT JOIN (
            -- 获取维度数据快照
            SELECT
                snick, employee_id, employee_name, department_id, department_name
            FROM (
                SELECT snick, employee_id, employee_name, department_id
                FROM (
                    -- 查询对应企业-平台的所有子账号及其部门ID, 不论其是否绑定员工
                    SELECT snick, department_id, employee_id
                    FROM ods.xinghuan_employee_snick_all
                    WHERE day = {snapshot_ds_nodash}
                ) AS snick_info
                GLOBAL LEFT JOIN (
                    SELECT
                        _id AS employee_id, username AS employee_name
                    FROM ods.xinghuan_employee_all
                    WHERE day = {snapshot_ds_nodash}
                ) AS employee_info
                USING(employee_id)
            ) AS snick_info
            GLOBAL LEFT JOIN (
                SELECT
                    _id AS department_id,
                    full_name AS department_name
                FROM xqc_dim.snick_department_full_all
                WHERE day = {snapshot_ds_nodash}
            ) AS department_info
            USING (department_id)
        ) AS dim_snick_department
        USING(snick)