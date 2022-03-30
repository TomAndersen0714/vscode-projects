-- ods.qc_case_label_detail_all
SELECT
    a.company_id AS company_id,
    a.name AS name,
    sum (b.label_count) AS label_count
FROM (
        with (
            select max(date)
            FROM ods.qc_case_label_detail_all
            WHERE company_id = '%s'
        ) -- companyId.Hex()
        as max_date
        SELECT DISTINCT `date`,
            company_id,
            concat(
                parent_label_name,
                if(
                    label_name = '',
                    label_name,
                    concat('/', label_name)
                )
            ) AS name
        FROM ods.qc_case_label_detail_all
        WHERE company_id = '%s' -- companyId.Hex()
            and date = max_date
    ) AS a
    LEFT JOIN (
        with (
            select max(date)
            FROM ods.qc_case_label_detail_all
            WHERE company_id = '%s'
        ) as max_date -- companyId.Hex()
        SELECT `date`,
            company_id,
            concat(
                parent_label_name,
                if(
                    label_name = '',
                    label_name,
                    concat('/', label_name)
                )
            ) AS name,
            sum(IF (dialog_id = '', 0, 1)) AS label_count
        FROM ods.qc_case_label_detail_all
        WHERE shop_name IN %s
            and date = max_date -- shopStr
        GROUP BY `date`,
            company_id,
            name
    ) AS b ON a.company_id = b.company_id
    AND a.name = b.name
    and a.date = b.date
GROUP by company_id,
    name