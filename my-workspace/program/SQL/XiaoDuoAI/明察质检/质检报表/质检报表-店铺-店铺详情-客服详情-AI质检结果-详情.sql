-- AI质检结果-详情
select
    seller_nick as shop_name, -- 店铺名称
    platform, -- 平台
    snick, -- 子账号名称
    cnick, -- 顾客名称
    abnormals_type_1 as non_custom_over, -- 非客服结束会话
    abnormals_type_2 as leak_follow, -- 漏跟进
    abnormals_type_3 as shortcut_repeat, -- 快捷语重复
    abnormals_type_4 as blunt_refusal, -- 生硬拒绝
    abnormals_type_5 as lack_comfort, -- 欠缺安抚
    abnormals_type_6 as random_reply, -- 答非所问
    abnormals_type_7 as word_reply, -- 单词回复/单个词语回复
    abnormals_type_8 as sentence_slow, -- 单句响应慢
    abnormals_type_9 as product_unfamiliar, -- 产品不熟悉
    abnormals_type_10 as activity_unfamiliar, -- 活动不熟悉
    abnormals_type_11 as inside_slow,
    abnormals_type_12 as serious_timeout, -- 回复严重超时
    abnormals_type_13 as withdraw_msg, -- 撤回消息
    abnormals_type_14 as single_emoji_reply, -- 单表情回复
    abnormals_type_15 as abnormal_withdraw_msg, -- 异常撤回
    abnormals_type_16 as no_answer_before, -- 转接前未有效回复
    _id as dialog_id
from (
        select *
        from ods.qc_detail_all as detail
            left join ods.xinghuan_employee_snick_all as employee on detail.snick = employee.snick
            and toInt32(toYYYYMMDD(detail.`date`)) = employee.day
        where employee_id = '%s'
            and seller_nick = '%s'
            and snick = '%s'
            and platform = '%s'
            and abnormals_count > 0
            and `date` >= %d
            and `date` <= %d -- (req.EmployeeId, req.ShopName, req.Snick, sess.Platform, req.StartDate, req.EndDate)
    ) as info
    left join ods.xinghuan_employee_all as temp on info.employee_id = temp._id
    and info.day = temp.day
where has(%s, department_id) -- service.BuildSqlArrayById(departmentIds)