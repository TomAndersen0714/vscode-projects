insert into ods.qc_detail_all
select t1.*,
    t2.tag_json_list
from (
        SELECT toDate('{ds}') AS `day`,
            a.platform,
            a.seller_nick as seller_nick,
            a.`group` as group,
            a.snick as snick,
            a._id as _id,
            a.cnick,
            a.mark,
            arraySum(a.abnormals_count) AS abnormals_count,
            arraySum(a.excellents_count) AS excellents_count,
            length(a.read_mark) AS read_mark_count,
            a.score,
            a.score_add,
            a.mark_score,
            a.mark_score_add,
            b.username as username,
            a.abnormals_count [1] AS abnormals_type_1,
            a.abnormals_count [2] AS abnormals_type_2,
            a.abnormals_count [3] AS abnormals_type_3,
            a.abnormals_count [4] AS abnormals_type_4,
            a.abnormals_count [5] AS abnormals_type_5,
            a.abnormals_count [6] AS abnormals_type_6,
            a.abnormals_count [7] AS abnormals_type_7,
            a.abnormals_count [8] AS abnormals_type_8,
            a.abnormals_count [9] AS abnormals_type_9,
            a.abnormals_count [10] AS abnormals_type_10,
            a.abnormals_count [11] AS abnormals_type_11,
            a.abnormals_count [12] AS abnormals_type_12,
            a.abnormals_count [13] AS abnormals_type_13,
            a.abnormals_count [14] AS abnormals_type_14,
            a.abnormals_count [15] AS abnormals_type_15,
            a.abnormals_count [16] AS abnormals_type_16,
            a.tag_score_stats_id AS tag_score_stats_id,
            a.tag_score_stats_score AS tag_score_stats_score,
            a.tag_score_add_stats_id AS tag_score_add_stats_id,
            a.tag_score_add_stats_score AS tag_score_add_stats_score,
            a.rule_stats_id AS rule_stats_id,
            a.rule_stats_score AS rule_stats_score,
            a.rule_stats_count AS rule_stats_count,
            a.rule_add_stats_id AS rule_add_stats_id,
            a.rule_add_stats_score AS rule_add_stats_score,
            a.rule_add_stats_count AS rule_add_stats_count
        FROM (
                select *
                from dwd.xdqc_dialog_all
                WHERE toYYYYMMDD(begin_time) = { ds_nodash }
            ) AS a 
            GLOBAL LEFT JOIN (
                select
                    account_info.account_id as account_id,
                    employee_info.username as username
                from (
                        select _id as account_id,
                            employee_id
                        from ods.xinghuan_account_all
                        where day = { ds_nodash }
                    ) as account_info
                    GLOBAL left join (
                        select
                            _id as employee_id,
                            username
                        from ods.xinghuan_employee_all
                        where day = { ds_nodash }
                    ) as employee_info
                    using(employee_id)
            ) AS b
            ON a.last_mark_id = b.account_id
    ) as t1 
    GLOBAL left join (
        SELECT day,
            snick,
            dialog_id,
            """ + """ groupArray(
                concat(
                    '{"tag_id":"',
                    tag_id,
                    '","tag_name":"',
                    `name`,
                    '","tag_score":',
                    toString(score),
                    ',"cal_op":',
                    toString(cal_op),
                    '}'
                )
            ) as tag_json_list """ + f"""
        FROM ods.xinghuan_dialog_tag_score_all
        WHERE day = { ds_nodash }
        group by day,
            snick,
            dialog_id
    ) as t2
    on t1._id = t2.dialog_id