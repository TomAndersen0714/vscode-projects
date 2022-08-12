select id
from (
        select id,
            any(warehouse) as warehouse1,
            any(logistics_company_abbr) as logistics_company_abbr1
        from sxx_ods.compensate_update_all
        where day between 20220727 and 20220810
            and warehouse <> '未知'
        group by id
    ) as t1
where id in (
        select id
        from sxx_dwd.compensate_workorder_all
        where day between 20220727 and 20220810
            and warehouse = '未知'
    )
    and id not in (
        select id
        from sxx_dwd.compensate_workorder_all
        where day between 20220727 and 20220810
            and warehouse <> '未知'
    )