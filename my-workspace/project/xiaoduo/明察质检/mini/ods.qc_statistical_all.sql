-- AI质检+人工质检+自定义质检-各子账号(客服)质检触发次数和分值统计
-- PS: 未统计未绑定员工的子账号
-- dwd.xdqc_dialog_all
-- ods.xinghuan_employee_snick_all
-- ods.xinghuan_employee_all
-- ods.xinghuan_department_all
-- ods.xinghuan_dialog_tag_score_all
-- ods.xinghuan_employee_all
-- ods.xinghuan_account_all

insert into ods.qc_statistical_all
select a.day,
    a.platform,
    b.company_id,
    b.department_id,
    b.department_name,
    b.employee_id,
    b.employee_name,
    a.snick,
    a.`group`,
    a.seller_nick,
    a.sessionc_count,
    a.qc_count,
    a.mark_score,
    a.mark_score_add,
    a.abnormals_count, -- AI质检扣分会话总量
    a.excellents_count, -- AI质检加分会话总量
    a.abnormals_sum,
    a.excellents_sum,
    a.read_mark_count, -- 人工质检-人工质检会话数
    a.tag_score_stats_count, -- 人工质检-扣分会话数
    a.tag_score_add_stats_count, -- 人工质检-加分会话数
    a.rule_score_stats_count, -- 自定义质检-扣分会话数
    a.rule_score_add_stats_count, -- 自定义质检-加分会话数
    a.mark_list,  -- 质检员名单
    a.tag_json_list
from (
        select t1.*,
            t2.tag_json_list
        from (
                SELECT toDate('{ds}') as `day`,
                    a.platform as platform,
                    a.snick as snick,
                    a.`group` as `group`,
                    a.seller_nick as seller_nick,
                    count(1) as sessionc_count,
                    count(1) as qc_count,
                    sum(a.mark_score) as mark_score,
                    sum(a.mark_score_add) as mark_score_add,
                    sum(if(arraySum(a.abnormals_count) > 0, 1, 0)) as abnormals_count,
                    sum(if(arraySum(a.excellents_count) > 0, 1, 0)) as excellents_count,
                    sum(arraySum(a.abnormals_count)) as abnormals_sum,
                    sum(arraySum(a.excellents_count)) as excellents_sum,
                    sum(if(length(mark_ids) != 0, 1, 0)) as read_mark_count,
                    sum(if(length(a.tag_score_stats_id) > 0, 1, 0)) as tag_score_stats_count,
                    sum(if(length(a.tag_score_add_stats_id) > 0, 1, 0)) as tag_score_add_stats_count,
                    sum(if(length(a.rule_stats_id) > 0, 1, 0)) as rule_score_stats_count,
                    sum(if(length(a.rule_add_stats_id) > 0, 1, 0)) as rule_score_add_stats_count,
                    arrayReduce('groupUniqArray', groupArray(b.username)) as mark_list
                FROM (
                    select *
                    from dwd.xdqc_dialog_all
                    WHERE toYYYYMMDD(begin_time) = { ds_nodash }
                ) as a
                -- 获取质检员姓名
                GLOBAL left join (
                    select
                        account_info.account_id as account_id,
                        employee_info.username as username
                    from (
                        select _id as account_id,
                            employee_id
                        from ods.xinghuan_account_all
                        where day = { ds_nodash }
                    ) as account_info
                    left join (
                        select _id as employee_id,
                            username
                        from ods.xinghuan_employee_all
                        where day = { ds_nodash }
                    ) as employee_info 
                    using(employee_id)
                ) as b 
                on a.last_mark_id = b.account_id
                group by `day`,
                    a.platform,
                    a.snick,
                    a.`group`,
                    a.seller_nick
            ) t1 
            GLOBAL left join (
                select
                    day,
                    snick,
                    groupArray(tag_json_list) as tag_json_list
                from (
                    SELECT day,
                        snick,
                        concat(
                            '{"tag_id":"',
                            tag_id,
                            '","tag_name":"',
                            `name`,
                            '","tag_score":',
                            toString(score),
                            ',"total":',
                            toString(count(1)),
                            ',"cal_op":',
                            toString(cal_op),
                            '}'
                    ) as tag_json_list
                    FROM ods.xinghuan_dialog_tag_score_all
                    WHERE day = { ds_nodash }
                    group by `day`,
                        snick,
                        tag_id,
                        `name`,
                        cal_op,
                        score
                )
                group by day,
                    snick
            ) as t2 
            on t1.snick = t2.snick
    ) a
    left join ( -- check!
        -- 查询所有已绑定子账号的公司id,分组id,分组名,员工id,员工名,对应的子账号名
        -- PS: 未绑定员工的子账号不在查询结果中, 如果分组下的子账号都未绑定, 则其对应列只会显示null, 总行数为1
        SELECT a.company_id AS company_id,
            a._id AS department_id,
            a.name AS department_name,
            b.employee_id AS employee_id,
            b.employee_name AS employee_name,
            b.snick AS snick
        FROM (
                -- 查询所有的子账号分组信息
                select *
                from ods.xinghuan_department_all
                where day = { ds_nodash }
            ) AS a
            GLOBAL LEFT JOIN (
                -- 查询所有已绑定子账号的员工id,分组id,员工名,子账号名
                -- PS: 没有绑定子账号的员工, 其分组id为null
                SELECT a._id AS employee_id,
                    b.department_id AS department_id,
                    a.username AS employee_name,
                    b.snick AS snick
                FROM(
                    -- 查询所有的员工信息
                    select *
                    from ods.xinghuan_employee_all
                    where day = { ds_nodash }
                ) AS a 
                GLOBAL LEFT JOIN (
                    -- 查询所有的子账号
                    -- PS: 未绑定员工的子账号, employee_id='000000000000000000000000', 即无法配对
                    select *
                    from ods.xinghuan_employee_snick_all
                    where day = { ds_nodash }
                        and platform = 'tb'
                ) AS b 
                ON a._id = b.employee_id
            ) AS b 
            ON a._id = b.department_id
    ) b
    on a.snick = b.snick