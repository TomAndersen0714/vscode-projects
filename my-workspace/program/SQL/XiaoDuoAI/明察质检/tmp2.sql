        SELECT
            compensate_warehouse_type_info.*,
            if(0 or (warehouse in ['代发仓', '萧山协作仓', '虚拟仓']) or (reason_level_3 in ['好评返现', '超时发货', '退差价', '仓库丢单', '修改差评', '直播中奖']) or (custom_goods in ['意大利面', '外婆菜', '海鲜卡', '火锅底料', '谢云提货', '鸭脖', '大闸蟹', '蟹卡']), 'false', 'true') AS is_cost
        FROM (
            WITH (
                SELECT groupArray(warehouse_type)
                FROM sxx_dim.jd_warehouse_map_all
                WHERE snapshot_day = 20220714
            ) AS jd_warehouses
            SELECT
                compensate_refund_way_info.*,
                if(
                    has(jd_warehouses, warehouse), 
                    '京东仓', 
                    if(warehouse='', '未知', warehouse)
                ) AS warehouse_type
            FROM (
                SELECT
                    compensate_responsible_info.*,
                    if(
                        compensate_way.refund_way='',
                        '线上',
                        compensate_way.refund_way
                    ) AS refund_way
                FROM (
                    SELECT
                        compensate_warehouse_info.*,
                        responsible_party
                    FROM (
                        SELECT
                            compensate_workorder_all.*,
                            if(
                                outbound_workorder_info.warehouse='',
                                '未知',
                                outbound_workorder_info.warehouse
                            ) AS warehouse,
                            if(
                                outbound_workorder_info.logistics_company='',
                                '未知',
                                outbound_workorder_info.logistics_company
                            ) AS logistics_company,
                            if(
                                outbound_workorder_info.logistics_company_abbr='',
                                '未知',
                                outbound_workorder_info.logistics_company_abbr
                            ) AS logistics_company_abbr,
                            if(
                                outbound_workorder_info.receiving_area='',
                                '未知',
                                outbound_workorder_info.receiving_area
                            ) AS receiving_area,
                            if(
                                outbound_workorder_info.is_outbound_need_to_filter='',
                                'false',
                                outbound_workorder_info.is_outbound_need_to_filter
                            ) AS is_outbound_need_to_filter
                        FROM (
                            SELECT
                                *,
                                concat(
                                    reason_level_3,
                                    if(reason_level_4='/' OR reason_level_4='', '', '/'),
                                    if(reason_level_4='/' OR reason_level_4='', '', reason_level_4)
                                ) AS reason_level_3_4
                            FROM sxx_ods.compensate_workorder_all
                            WHERE day = 20220712
                            -- 筛选非批量打款工单
                            AND type != '批量打款工单'
                        ) AS compensate_workorder_all
                        GLOBAL LEFT JOIN (
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
                            WHERE day BETWEEN 20220113 AND 20220712
                            AND plat_order_id GLOBAL IN (
                                SELECT DISTINCT
                                    order_id
                                FROM sxx_ods.compensate_workorder_all
                                WHERE day = 20220712
                                -- 筛选非批量打款工单
                                AND type != '批量打款工单'
                            )
                        ) AS outbound_workorder_info
                        ON compensate_workorder_all.order_id = outbound_workorder_info.plat_order_id
                    ) AS compensate_warehouse_info
                    GLOBAL LEFT JOIN (
                        SELECT DISTINCT
                            compensate_reason_3,
                            compensate_reason_4,
                            responsible_party
                        FROM sxx_dim.responsible_party_map_all
                        WHERE snapshot_day = 20220714
                    ) AS responsible_party_map
                    ON compensate_warehouse_info.reason_level_3 = responsible_party_map.compensate_reason_3
                    AND compensate_warehouse_info.reason_level_4 = responsible_party_map.compensate_reason_4
                ) AS compensate_responsible_info
                GLOBAL LEFT JOIN (
                    SELECT DISTINCT
                        compensate_type,
                        refund_way
                    FROM sxx_dim.compensate_way_map_all
                    WHERE snapshot_day = 20220714
                ) AS compensate_way
                USING(compensate_type)
            ) AS compensate_refund_way_info
        ) AS compensate_warehouse_type_info