SELECT DISTINCT
    plat_order_id,
    warehouse,
    logistics_company,
    logistics_company_abbr,
    receiving_area,
    is_need_to_filter AS is_outbound_need_to_filter
FROM sxx_dwd.outbound_workorder_all
ARRAY JOIN
    splitByChar(',', origin_id) AS plat_order_id
WHERE day BETWEEN {start_ds_nodash} AND {ds_nodash}
AND plat_order_id GLOBAL IN (
    SELECT DISTINCT
        order_id
    FROM {ch_source_table}
    WHERE day = {ds_nodash}
    -- 筛选非批量打款工单
    AND type != '批量打款工单'
)