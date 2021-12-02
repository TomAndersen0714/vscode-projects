SELECT
    COUNT(1) AS `人工质检量`
FROM dwd.xdqc_dialog_all
WHERE toYYYYMMDD(begin_time) BETWEEN {{day.start}} AND {{day.end}}
AND seller_nick IN ['方太官方旗舰店','方太集成烹饪中心旗舰店']
AND mark_ids != []



SELECT
    /* toDate('{ds}') AS `date`,
    dialog_info.platform as platform,
    dim_info.company_id as company_id,
    '' as company_name,
    '' as department_id,
    '' as department_name,
    dialog_info.account_id as account_id,
    dim_info.username as username,
    dialog_info.seller_nick as shop_name,
    dialog_info.dialog_id as dialog_id, */
    COUNT(1)
FROM (
    SELECT
        platform,
        seller_nick,
        snick,
        _id as dialog_id,
        last_mark_id AS account_id
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{day.start}}')) AND toYYYYMMDD(toDate('{{day.end}}'))
    AND seller_nick IN ['方太官方旗舰店','方太集成烹饪中心旗舰店']
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
        WHERE day = toYYYYMMDD(yesterday())
    ) AS account_info
    GLOBAL LEFT JOIN (
        SELECT
            _id AS employee_id,
            username
        FROM ods.xinghuan_employee_all
        WHERE day = toYYYYMMDD(yesterday())
    ) AS employee_info 
    USING(employee_id)
) dim_info
USING(account_id)