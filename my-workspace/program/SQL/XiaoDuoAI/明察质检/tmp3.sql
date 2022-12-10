SELECT platform,
    BG,
    BU,
    shop_name,
    superior_name,
    employee_name,
    snick,
    cnick,
    dialog_id,
    message_id,
    alert_info.id AS alert_id,
    level,
    warning_type,
    time,
    time as warning_time,
    toInt64(
        if(
            is_finished = 'True',
            round(
                (
                    parseDateTimeBestEffort(if(finish_time != '', finish_time, toString(now()))) - parseDateTimeBestEffort(time)
                ) / 60
            ),
            round((now() - parseDateTimeBestEffort(time)) / 60)
        )
    ) AS warning_duration,
    finish_time,
    is_finished,
    if(notify_time != '', 'True', 'False') AS is_notified,
    notify_time,
    toInt64(
        if(
            notify_time != '',
            if(
                finish_time != '',
                toString(
                    round(
                        (
                            parseDateTimeBestEffort(if(notify_time != '', notify_time, toString(now()))) - parseDateTimeBestEffort(if(finish_time != '', finish_time, toString(now())))
                        ) / 60
                    )
                ),
                toString(
                    round(
                        (
                            now() - parseDateTimeBestEffort(if(notify_time != '', notify_time, toString(now())))
                        ) / 60
                    )
                )
            ),
            '0'
        )
    ) AS notify_duration
FROM (
        SELECT shop_info.company_id AS company_id,
            shop_info.bg_id AS bg_id,
            bg_info.department_name AS BG,
            shop_info.bu_id AS bu_id,
            bu_info.department_name AS BU,
            shop_info.department_id AS shop_id,
            shop_info.department_name AS shop_name
        FROM (
                SELECT parent_department_path [1] AS bg_id,
                    parent_department_path [2] AS bu_id,
                    parent_department_path,
                    company_id,
                    department_id,
                    department_name
                FROM xqc_dim.group_all
                WHERE is_shop = 'True'
            ) AS shop_info GLOBAL
            LEFT JOIN (
                SELECT department_id,
                    department_name
                FROM xqc_dim.group_all
                WHERE is_shop = 'False'
            ) AS bg_info ON shop_info.bg_id = bg_info.department_id GLOBAL
            LEFT JOIN (
                SELECT department_id,
                    department_name
                FROM xqc_dim.group_all
                WHERE is_shop = 'False'
            ) AS bu_info ON shop_info.bu_id = bu_info.department_id
        WHERE company_id = '6131e6554524490001fc6825'
    ) GLOBAL
    INNER JOIN(
        SELECT *
        FROM (
                SELECT alert_id,
                    time AS notify_time
                FROM xqc_ods.alert_remind_all
                WHERE shop_id IN ['61d6a38716bbc36cb34dfd4c','61d6a38716bbc36cb34dfd56','61d6a38716bbc36cb34dfd52','61c193262ba76f001d769b90','5f3cd79bb7fba70017c854bb','61de9efadba7c00020cfd5f5','61dd56c1df229d00176cdce8','6170ddb2abefdb000c773b0a','616d2b651ffab50014d6f922','6172894009841b000fafffc9','61ee6acf09f2f12c5f2f1852','61ee6acf09f2f12c5f2f182a','61ee6acf09f2f12c5f2f1839','616d49b11ffab50016d6fa49','616fccff269ebf000e1b88b0','616e207da08ae900109dcf33','616e1b70abefdb0010773a23','616d282d1ffab50012d6f485','61d6a38716bbc36cb34dfd48','61d6a38716bbc36cb34dfd58','61ee6acf09f2f12c5f2f1843','61d6a38716bbc36cb34dfd4a','61d6a38716bbc36cb34dfd4e','61e5018858da510015331810','61e5044057e4bb0013e7c8f2','61e504cc90454f001656481e','61e50a34614e070018f45a8e','616face4a08ae9000e9dd0a9','616f7c6d09841b000eaff41e','61c94f4f6383be001deb8e21','61ee6acf09f2f12c5f2f1834','61d6a38716bbc36cb34dfd46','61d6a38716bbc36cb34dfd50','61d6a38716bbc36cb34dfd54','61ee6acf09f2f12c5f2f1848','61ee6acf09f2f12c5f2f184d','61ee6acf09f2f12c5f2f182f','61ee6acf09f2f12c5f2f183e','6139c3c96ebd17000e94b5b5','6139e720fb530f0010c19481','613af5f56ebd17000f942ca2','6131c3766ebd17000a93c0cd','627b9d832ea7ee00179fc09d','614ae633fb530f0010c1b33f','5cd268e42bf9a8000f9301d7','614c21b16ebd170010947761','6139c118e16787000fb8a1cf','618ca3649416a3001c5f413d','62d65787e506f30018791b29']
                    and resp_code = 0
            ) AS alert_remind GLOBAL
            RIGHT JOIN (
                SELECT id AS alert_id,
                    *
                FROM xqc_ods.alert_all FINAL
                WHERE day BETWEEN 20220909 AND 20221207
                    AND (
                        shop_id IN ['61d6a38716bbc36cb34dfd4c','61d6a38716bbc36cb34dfd56','61d6a38716bbc36cb34dfd52','61c193262ba76f001d769b90','5f3cd79bb7fba70017c854bb