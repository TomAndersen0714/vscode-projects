

-- 客服详情信息-人工质检结果
select
    shop_name, -- 店铺名称
    snick, -- 子账号名称
    department_id, -- 分组名
    total_score, -- 总扣分
    total_score_add, -- 总加分
    toString(check_human) as check_human, -- 质检员
    name, -- 人工质检标签名
    tag_id,
    score,
    cal_op,
    tag_score
from (
        select seller_nick,
            snick,
            department_id,
            mark_list as check_human,
            name,
            tag_id,
            score,
            cal_op,
            tag_score
        from (
                select seller_nick,
                    snick,
                    department_id,
                    arrayReduce('groupUniqArray', flatten(groupArray(mark_list))) as mark_list
                from ods.qc_statistical_all
                where date between %d and %d
                    and employee_id = '%s' -- (req.StartDate, req.EndDate, req.EmployeeId)
                    and seller_nick = '%s' -- (req.ShopName)
                group by seller_nick,
                    snick,
                    department_id
            ) as qc_statistical
            left join (
                select snick,
                    name,
                    tag_id,
                    cal_op,
                    score,
                    sum(score) as tag_score
                from ods.xinghuan_dialog_tag_score_all
                where day between toInt32(toYYYYMMDD(toDate(%d))) and toInt32(toYYYYMMDD(toDate(%d))) -- (req.StartDate, req.EndDate)
                group by snick,
                    name,
                    tag_id,
                    cal_op,
                    score
            ) as tag_score on qc_statistical.snick = tag_score.snick
    ) as score
    left join (
        select seller_nick as shop_name,
            snick,
            department_id,
            sum(mark_score) as total_score,
            sum(mark_score_add) as total_score_add
        from ods.qc_statistical_all
        where employee_id = '%s'
            and `date` >= %d
            and `date` <= %d -- (req.EmployeeId, req.StartDate, req.EndDate)
        group by seller_nick,
            snick,
            department_id
    ) as tag on score.snick = tag.snick
    and score.seller_nick = tag.shop_name
    and score.department_id = tag.department_id
where has(%s, department_id) -- (service.BuildSqlArrayById(departmentIds))
limit %d offset %d -- req.PageSize, (req.CurrentPage-1)*req.PageSize