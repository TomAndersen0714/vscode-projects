-- AI质检-客服和买家质检词触发次数统计
insert into ods.qc_words_detail_all
SELECT
    toDate('{ds}'),
    words_info.platform,
    dim_info.company_id,
    '' AS company_name,
    dim_info.department_id,
    dim_info.department_name,
    dim_info.employee_id,
    dim_info.employee_name,
    words_info.shop_name,
    words_info.`group`,
    words_info.snick,
    words_info.source,
    words_info.word,
    words_info.words_count
FROM (
        -- 统计各个质检词的数量
        SELECT `date`,
            platform,
            seller_nick AS shop_name,
            `group`,
            snick,
            source,
            word,
            sum(count) AS words_count
        FROM dwd.xdqc_dialog_all 
        array JOIN 
            qc_word_word AS word,
            qc_word_source AS source,
            qc_word_count AS count
        WHERE toYYYYMMDD(begin_time) = { ds_nodash }
            AND qc_word_word != []
        GROUP BY `date`,
            platform,
            seller_nick,
            `group`,
            snick,
            source,
            word
    ) AS words_info 
    GLOBAL LEFT JOIN (
        SELECT a.company_id AS company_id,
            a._id AS department_id,
            a.name AS department_name,
            b.employee_id AS employee_id,
            b.employee_name AS employee_name,
            b.snick AS snick
        FROM (
                -- 查询所有的子账号分组
                SELECT *
                FROM ods.xinghuan_department_all
                WHERE day = { ds_nodash }
            ) AS a 
            GLOBAL LEFT JOIN (
                -- 查询所有相互绑定的员工和子账号, PS: 无法查询出未绑定员工的子账号
                SELECT a._id AS employee_id,
                    b.department_id AS department_id,
                    a.username AS employee_name,
                    b.snick AS snick
                FROM (
                        -- 查询所有的员工
                        SELECT *
                        FROM ods.xinghuan_employee_all
                        WHERE day = { ds_nodash }
                    ) AS a
                    GLOBAL LEFT JOIN (
                        -- 查询所有的子账号
                        SELECT *
                        FROM ods.xinghuan_employee_snick_all
                        WHERE day = { ds_nodash }
                            and platform = 'tb'
                    ) AS b 
                    ON a._id = b.employee_id
            ) AS b 
            ON a._id = b.department_id
    ) dim_info 
    on words_info.snick = dim_info.snick