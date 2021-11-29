select
    platform,
    _id as dialog_id,
    seller_nick as shop_name,
    snick,
    cnick,
    mark_score as score,
    mark_score_add as score_add,
    mark,
    toString(read_mark_id) as check_human,
    toString(tag_json_list) as tag_info
from (
    select *
    from ods.qc_detail_all as detail
    left join ods.xinghuan_employee_snick_all as employee
    on detail.snick = employee.snick
    and toInt32(toYYYYMMDD(detail.`date`)) = employee.day 
    where employee_id = '%s' 
    and seller_nick = '%s' 
    and snick = '%s' 
    and platform = '%s' 
    and `date` >= %d 
    and `date` <= %d 
    and tag_json_list != []
    -- req.EmployeeId, req.ShopName, req.Snick, sess.Platform, req.StartDate, req.EndDate
) as info
left join ods.xinghuan_employee_all as temp 
on info.employee_id = temp._id
and info.day = temp.day
where has(['%s'], department_id)