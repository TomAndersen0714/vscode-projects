    SELECT department_id AS shop_id
    FROM xqc_dim.group_all
    WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
    AND is_shop = 'True'
    -- AND platform = '{{ platform=jd }}'

        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
        -- AND platform = '{{ platform=jd }}'