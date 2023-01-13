-- 人工质检-各子账号人工质检加分标签明细汇总
-- dwd.xdqc_dialog_all
-- ods.xdqc_tag_all

insert into ods.xinghuan_dialog_tag_score_all
select a.seller_nick,
    a.`group`,
    a.snick,
    a._id,
    a.cnick,
    a.tag_score_add_stats_id,
    b.name,
    b.score,
    1 as cal_op,
    { ds_nodash }
from (
        select
            seller_nick,
            `group`,
            snick,
            _id,
            cnick,
            tag_score_add_stats_id
        FROM dwd.xdqc_dialog_all
        array join
            tag_score_add_stats_id
        WHERE toYYYYMMDD(begin_time) = { ds_nodash }
            and length(tag_score_add_stats_id) > 0
    ) as a
    left join (
        select *
        from ods.xdqc_tag_all
        where day = { ds_nodash }
    ) as b 
    on a.tag_score_add_stats_id = b._id
    and a.group = b.group

-- 人工质检-各子账号人工质检减分标签明细汇总
insert into ods.xinghuan_dialog_tag_score_all
select a.seller_nick,
    a.`group`,
    a.snick,
    a._id,
    a.cnick,
    a.tag_score_stats_id,
    b.name,
    b.score,
    0 as cal_op,
    { ds_nodash }
from (
        select
            seller_nick,
            `group`,
            snick,
            _id,
            cnick,
            tag_score_stats_id
        FROM dwd.xdqc_dialog_all 
        array join 
            tag_score_stats_id
        WHERE toYYYYMMDD(begin_time) = { ds_nodash }
        and length(tag_score_stats_id) > 0
    ) as a
    left join (
        select *
        from ods.xdqc_tag_all
        where day = { ds_nodash }
    ) as b 
    on a.tag_score_stats_id = b._id
    and a.group = b.group

