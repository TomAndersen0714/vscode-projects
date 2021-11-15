-- AI质检+人工质检-各客服质检问题汇总统计

-- AI质检问题-扣分行为统计
-- dwd.xdqc_dialog_all
-- ods.xinghuan_employee_snick_all
-- ods.xinghuan_employee_all
-- ods.xinghuan_department_all
insert into ods.qc_question_detail_all
SELECT toDate('{ds}'),
    ai_qc_info.platform,
    dim_info.company_id,
    '' AS company_name,
    dim_info.department_id,
    dim_info.department_name,
    dim_info.employee_id,
    dim_info.employee_name,
    ai_qc_info.seller_nick as shop_name,
    ai_qc_info.`group`,
    ai_qc_info.`type`,
    ai_qc_info.qc_id,
    ai_qc_info.qc_name,
    ai_qc_info.qc_count
from (
        select `seller_nick`,
            platform,
            `group`,
            'ai' as type,
            snick,
            qc_id,
            '' as qc_name,
            qc_count
        from dwd.xdqc_dialog_all
        array join 
            abnormals_type as qc_id,
            abnormals_count as qc_count
        where toYYYYMMDD(begin_time) = { ds_nodash }
            and qc_count != 0
            and platform = 'tb'
    ) ai_qc_info
    left join (
        SELECT a.company_id AS company_id,
            a._id AS department_id,
            a.name AS department_name,
            b.employee_id AS employee_id,
            b.employee_name AS employee_name,
            b.snick AS snick
        FROM (
                SELECT *
                FROM ods.xinghuan_department_all
                WHERE day = { ds_nodash }
            ) AS a GLOBAL
            LEFT JOIN (
                SELECT a._id AS employee_id,
                    b.department_id AS department_id,
                    a.username AS employee_name,
                    b.snick AS snick
                FROM (
                        SELECT *
                        FROM ods.xinghuan_employee_all
                        WHERE day = { ds_nodash }
                    ) AS a GLOBAL
                    LEFT JOIN (
                        SELECT *
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = { ds_nodash }
                            and platform = 'tb'
                    ) AS b ON a._id = b.employee_id
            ) AS b ON a._id = b.department_id
    ) dim_info on ai_qc_info.snick = dim_info.snick

-- 人工质检-标签统计
-- ods.xinghuan_dialog_tag_score_all
-- ods.xinghuan_employee_snick_all
-- ods.xinghuan_employee_all
-- ods.xinghuan_department_all
-- ods.xdqc_tag_sub_category_all
-- ods.xinghuan_qc_norm_all
-- ods.xdqc_tag_all

insert into ods.qc_question_detail_all
SELECT toDate('{ds}'),
    'tb' as platform,
    dim_info.company_id,
    '' AS company_name,
    dim_info.department_id,
    dim_info.department_name,
    dim_info.employee_id,
    dim_info.employee_name,
    manual_qc_info.seller_nick as shop_name,
    manual_qc_info.`group`,
    manual_qc_info.`type`,
    manual_qc_info.qc_id,
    manual_qc_info.qc_name,
    manual_qc_info.qc_count
from (
        select tag_info.seller_nick,
            tag_info.`group`,
            'manual' as type,
            tag_info.snick,
            tag_info.tag_id as qc_id,
            all_tag_name_info.all_tag_name as qc_name,
            tag_info.qc_count as qc_count
        from (
                select tag_id,
                    seller_nick,
                    `group`,
                    snick,
                    count(1) as qc_count
                from ods.xinghuan_dialog_tag_score_all
                where day = { ds_nodash }
                    and cal_op = 0
                group by tag_id,
                    seller_nick,
                    `group`,
                    snick
            ) as tag_info
            left join (
                select norm_tag.tag_id,
                    toString(
                        concat(
                            if(
                                norm_tag.qc_norm_name = '',
                                '未设置一级标签',
                                norm_tag.qc_norm_name
                            ),
                            '/',
                            if(
                                sub_category.name = '',
                                '未设置二级标签',
                                sub_category.name
                            ),
                            '/',
                            norm_tag.tag_name
                        )
                    ) as all_tag_name
                from (
                        select b._id as qc_norm_id,
                            b.name as qc_norm_name,
                            a._id as tag_id,
                            a.name as tag_name,
                            a.sub_category_id as sub_category_id
                        from (
                                select _id,
                                    category_id,
                                    sub_category_id,
                                    seller_nick,
                                    qc_norm_id,
                                    name
                                from ods.xdqc_tag_all
                                where day = { ds_nodash }
                            ) as a
                            left join (
                                select _id,
                                    name
                                from ods.xinghuan_qc_norm_all
                                where day = { ds_nodash }
                                    and status = 1
                            ) as b on a.qc_norm_id = b._id
                    ) as norm_tag
                    left join (
                        select _id,
                            name
                        from ods.xdqc_tag_sub_category_all
                        where day = { ds_nodash }
                    ) as sub_category on norm_tag.sub_category_id = sub_category._id
            ) as all_tag_name_info on tag_info.tag_id = all_tag_name_info.tag_id
    ) as manual_qc_info
    left join (
        SELECT a.company_id AS company_id,
            a._id AS department_id,
            a.name AS department_name,
            b.employee_id AS employee_id,
            b.employee_name AS employee_name,
            b.snick AS snick
        FROM (
                SELECT *
                FROM ods.xinghuan_department_all
                WHERE day = { ds_nodash }
            ) AS a GLOBAL
            LEFT JOIN (
                SELECT a._id AS employee_id,
                    b.department_id AS department_id,
                    a.username AS employee_name,
                    b.snick AS snick
                FROM (
                        SELECT *
                        FROM ods.xinghuan_employee_all
                        WHERE day = { ds_nodash }
                    ) AS a GLOBAL
                    LEFT JOIN (
                        SELECT *
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = { ds_nodash }
                            and platform = 'tb'
                    ) AS b ON a._id = b.employee_id
            ) AS b ON a._id = b.department_id
    ) dim_info on manual_qc_info.snick = dim_info.snick
    

-- 客服情绪问题
insert into ods.qc_question_detail_all
SELECT toDate('{ds}'),
    ai_qc_info.platform,
    dim_info.company_id,
    '' AS company_name,
    dim_info.department_id,
    dim_info.department_name,
    dim_info.employee_id,
    dim_info.employee_name,
    ai_qc_info.seller_nick as shop_name,
    ai_qc_info.`group`,
    ai_qc_info.`type`,
    ai_qc_info.qc_id,
    ai_qc_info.qc_name,
    ai_qc_info.qc_count
from (
        select `seller_nick`,
            platform,
            `group`,
            's_emotion' as type,
            snick,
            qc_id,
            '' as qc_name,
            qc_count
        from dwd.xdqc_dialog_all array
            join s_emotion_type as qc_id,
            s_emotion_count as qc_count
        where toYYYYMMDD(begin_time) = { ds_nodash }
            and qc_count != 0
            and platform = 'tb'
    ) ai_qc_info
    left join (
        SELECT a.company_id AS company_id,
            a._id AS department_id,
            a.name AS department_name,
            b.employee_id AS employee_id,
            b.employee_name AS employee_name,
            b.snick AS snick
        FROM (
                SELECT *
                FROM ods.xinghuan_department_all
                WHERE day = { ds_nodash }
            ) AS a GLOBAL
            LEFT JOIN (
                SELECT a._id AS employee_id,
                    b.department_id AS department_id,
                    a.username AS employee_name,
                    b.snick AS snick
                FROM (
                        SELECT *
                        FROM ods.xinghuan_employee_all
                        WHERE day = { ds_nodash }
                    ) AS a GLOBAL
                    LEFT JOIN (
                        SELECT *
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = { ds_nodash }
                            and platform = 'tb'
                    ) AS b ON a._id = b.employee_id
            ) AS b ON a._id = b.department_id
    ) dim_info on ai_qc_info.snick = dim_info.snick