select
    a.cust_id,
    '建立业务关系时间早于导入时间,命中时间比导入时间晚于2天' as type,
    a.hit_dt,
    c.import_date,
    a.blacklist_id
from
    (
        select
            hit_dt,
            party_id as cust_id,
            status,
            blacklist_id,
            uuid
        from
            imd_aml_safe.rrs_aml_blacklist_final_hit_status
        where
            ds = '2023-12-02'
    ) a
    inner join (
        select
            cust_id,
            min(start_date) as start_date
        from
            imd_aml_safe.rrs_aml_wash_cust_product_info_fact -- 建立业务关系时间
        where
            ds = '2023-12-02'
            and nvl(start_date, '') <> ''
        group by
            cust_id
    ) b -- 有效客户
    on a.cust_id = b.cust_id
    left join (
        select
            uuid,
            substr(create_dt, 1, 10) as import_date,
            substr(update_dt, 1, 10) as update_dt
        from
            imd_aml_safe.rrs_aml_t07_blacklist_batch_full
        where
            ds = '2023-12-02'
        union
        all
        select
            id as uuid,
            substr(create_dt, 1, 10) as import_date,
            substr(update_dt, 1, 10) as update_dt
        from
            imd_aml_safe.rrs_aml_black_web_fact
        where
            ds = '2023-12-02'
    ) c on a.blacklist_id = c.uuid
    left join (
        select
            *
        from
            imd_aml_safe.rrs_aml_final_hit_status_log
        where
            ds = '2023-12-02'
            and nvl(list_op_reason, '') <> ''
    ) hit_log on a.uuid = hit_log.uuid
where
    datediff(a.hit_dt, c.import_date) > 2
    and b.start_date < c.import_date
    and a.blacklist_id != ''
    and hit_log.uuid is null
    and a.hit_dt > '2022-09-12'
group by
    a.cust_id