-- PS: 由于部门已经修改为子账号分组, 部门信息将不再需要
INSERT INTO ods.qc_read_mark_detail_all
SELECT
    toDate('{ds}') AS `date`,
    dialog_info.platform as platform,
    dim_info.company_id as company_id,
    '' as company_name,
    '' as department_id,
    '' as department_name,
    dialog_info.account_id as account_id,
    dim_info.username as username,
    dialog_info.seller_nick as shop_name,
    dialog_info.dialog_id as dialog_id
FROM (
    SELECT
        platform,
        seller_nick,
        snick,
        _id as dialog_id,
        last_mark_id AS account_id
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) = { ds_nodash }
    AND last_mark_id != ''
) as dialog_info
GLOBAL LEFT JOIN (
    SELECT
        account_info.company_id AS company_id,
        account_info.account_id AS account_id,
        employee_info.username AS username
    FROM (
        SELECT
            _id AS account_id,
            employee_id,
            company_id
        FROM ods.xinghuan_account_all
        WHERE day = { ds_nodash }
    ) AS account_info
    GLOBAL LEFT JOIN (
        SELECT
            _id AS employee_id,
            username
        FROM ods.xinghuan_employee_all
        WHERE day = { ds_nodash }
    ) AS employee_info 
    USING(employee_id)
) dim_info
USING(account_id)