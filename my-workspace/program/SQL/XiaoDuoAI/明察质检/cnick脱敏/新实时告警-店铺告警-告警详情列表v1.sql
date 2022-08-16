-- 新实时告警-店铺告警-告警详情列表
SELECT
    seller_nick AS `店铺`, -- 店铺
    department_name AS `子账号分组`,
    superior_name AS `客服负责人`, -- 负责人
    employee_name AS `客服`, -- 客服
    snick AS `子账号`, -- 子账号
    cnick AS `顾客`, -- 顾客
    CASE
        WHEN level=1 THEN '初级告警'
        WHEN level=2 THEN '中级告警'
        WHEN level=3 THEN '高级告警'
        ELSE '其他'
    END AS `告警等级`, -- 告警等级
    warning_type AS `告警内容`, -- 告警内容
    time AS `告警时间`, -- 告警时间
    toInt64(if(
        is_finished='True',
        round((parseDateTimeBestEffort(if(finish_time!='',finish_time,toString(now()))) - parseDateTimeBestEffort(time))/60),
        round((now() - parseDateTimeBestEffort(time))/60)
    )) AS `告警时长(min)`, -- 告警时长(min)
    finish_time AS `处理完成时间`, -- 处理完成时间
    if(is_finished='True','已处理','未处理') AS `处理状态`, -- 处理状态
    dialog_id, -- 会话ID(反查)
    message_id, -- 消息ID(反查)
    id AS alert_id, -- 告警ID(反查)
    open_uid, -- 反查
    buyer_one_id, -- 反查
    platform, day, shop_id
FROM (
    SELECT *
    FROM xqc_ods.alert_all FINAL
        -- 已订阅店铺
    WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=today }}')) 
        AND toYYYYMMDD(toDate('{{ day.end=today }}'))
    -- 过滤旧版标准
    AND level IN [1,2,3]
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        AND platform = '{{ platform=tb }}'
    )
    -- 权限隔离
    AND (
        shop_id IN splitByChar(',','{{ shop_id_list=5f747ba42c90fd0001254404 }}')
        OR
        snick IN splitByChar(',','{{ snick_list=null }}')
    )
    -- 下拉框-平台
    AND platform = '{{ platform=tb }}'
    -- 下拉框-店铺
    AND (
        '{{ shop_ids }}' = ''
        OR
        shop_id IN splitByChar(',','{{ shop_ids }}')
    )
    -- 下拉框-告警等级
    AND (
        '{{ levels }}' = ''
        OR
        toString(level) IN splitByChar(',','{{ levels }}')
    )
    -- 下拉框-告警项
    AND (
        '{{ warning_types }}' = ''
        OR
        warning_type IN splitByChar(',','{{ warning_types }}')
    )
    -- 下拉框-告警项处理状态
    AND (
        '{{ alert_state }}' = ''
        OR
        is_finished IN splitByChar(',','{{ alert_state }}')
    )
    -- 筛选框-文本框内容
    AND (
        superior_name LIKE '%{{ search_string }}%'
        OR
        snick LIKE '%{{ search_string }}%'
        OR
        cnick LIKE '%{{ search_string }}%'
    )
) AS alert_info
GLOBAL LEFT JOIN (
    -- 关联子账号分组/子账号员工信息
    SELECT
        snick, department_id, department_name
    FROM (
        -- 查询对应企业-平台的所有子账号及其部门ID, 不论其是否绑定员工
        SELECT snick, department_id
        FROM ods.xinghuan_employee_snick_all
        WHERE day = toYYYYMMDD(yesterday())
        AND platform = '{{ platform=tb }}'
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    ) AS snick_info
    GLOBAL LEFT JOIN (
        SELECT
            _id AS department_id, full_name AS department_name
        FROM xqc_dim.snick_department_full_all
        WHERE day = toYYYYMMDD(yesterday())
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    ) AS department_info
    USING (department_id)
) AS dim_snick_department
USING(snick)
order by time desc