-- 创建临时表, 存储演算数据
CREATE TABLE tmp.qc_read_mark_detail
AS ods.qc_read_mark_detail_all
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(date)
ORDER BY (company_id, username)
SETTINGS index_granularity = 8192, storage_policy = 'rr'

-- 将修改后的昨天数据写入到临时表
insert into tmp.qc_read_mark_detail
select 
    toDate('2021-12-06') AS `day`,
    dialog_info.platform as platform,
    dim_info.company_id as company_id ,
    '' as company_name,
    dim_info.department_id as department_id,
    dim_info.department_name  as department_name,
    dialog_info.last_mark_id as account_id,
    dim_info.username as username,
    dialog_info.seller_nick as seller_nick,
    dialog_info.dialog_id as dialog_id
from (
    select platform,seller_nick,snick,_id as dialog_id ,last_mark_id
    from dwd.xdqc_dialog_all 
    where toYYYYMMDD(begin_time) = 20211206 and last_mark_id != ''
) as dialog_info 
left join (
    SELECT
        account.company_id AS company_id,
        department._id AS department_id,
        department.name AS department_name,
        account.account_id AS account_id,
        account.username AS username
    FROM (
            select
                a_employee.company_id AS company_id,
                a_employee.account_id AS account_id,
                a_employee.username AS username,
                a_employee.employee_id as employee_id,
                e_snick.department_id as department_id
            from (
                SELECT
                    account_info.company_id AS company_id,
                    account_info.account_id AS account_id,
                    employee_info.username AS username,
                    account_info.employee_id AS employee_id
                FROM (
                    SELECT 
                        company_id,
                        _id AS account_id,
                        employee_id
                    FROM ods.xinghuan_account_all
                    WHERE day = 20211206
                ) AS account_info
                LEFT JOIN (
                    SELECT
                        _id AS employee_id,
                        username
                    FROM ods.xinghuan_employee_all
                    WHERE day = 20211206
                ) AS employee_info 
                USING(employee_id)
        ) as a_employee
        left join (
            select 
                company_id,
                department_id,
                employee_id 
            from ods.xinghuan_employee_snick_all  
            WHERE day = 20211206  
        ) as e_snick 
        using(employee_id)
    ) AS account
    LEFT JOIN (
        SELECT
            _id,
            company_id,
            name
        FROM ods.xinghuan_department_all
        WHERE day = 20211206
    ) AS department
    ON department._id = account.department_id
) dim_info 
on dialog_info.last_mark_id  = dim_info.account_id


-- 执行后端提交的查询, 观察查询统计结果, 与线上结果进行对比
select
    'read_mark' as type, 
    account_id as employee_id, 
    username as employee_name, 
    0 as total_count, 
    count(1) as total_check, 
    0 as abnormal_score, 
    0 as abnormal_rate, 
    0 as ai_abnormal_count, 
    0 as human_abnormal_count, 
    0 as human_total_check, 
    count(1)/if(dateDiff('day', toDate(1638720000), toDate(1638806399))=0,1, dateDiff('day', toDate(1638720000), toDate(1638806399))) as average_check,
    0 as user_rule_score, 
    0 as avg_score 
from tmp.qc_read_mark_detail
where username != ''
and date >= 1638720000 and date < 1638806399
and shop_name in ['方太官方旗舰店','方太集成烹饪中心旗舰店']
and employee_name != '' 
group by account_id,username 
order by total_check desc 
limit 10

-- 自测完成之后, 提交gitlab代码