-- 子账号分组会话量

-- 1. 统计各个子账号当天会话量
-- PS: 目前缺少子账号和对应分组的映射, 需要先修改表结构, 然后从融合版迁移维度表历史数据
-- 到老淘宝, 以及拉取最新的维度数据 ods.xinghuan_employee_snick_all
SELECT
    snick, -- 子账号名
    COUNT(1) AS snick_today_dialog_cnt -- 店铺当天会话量
FROM xqc_ods.dialog_all
WHERE day = today
-- 组织架构包含店铺
AND shop_id GLOBAL IN (
    SELECT department_id AS shop_id
    FROM xqc_dim.group_all
    WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
    AND is_shop = 'True'
    AND platform = '{{ platform=tb }}'
)

/* -- 已订阅店铺
-- PS: 和组织架构所包含店铺二选一
AND shop_id GLOBAL IN (
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
    AND platform = '{{ platform=tb }}'
) */
-- 权限隔离
AND (
        shop_id IN splitByChar(',','{{ shop_id_list=615faf72b0c5f1001957c249 }}')
        OR
        snick IN splitByChar(',','{{ snick_list=NULL }}')
    )
GROUP BY snick

-- 2. 获取子账号与子账号分组的T+1映射
-- PS: 由于子账号分组最多有3层, 因此需要进行2次JOIN, 来获取子账号分组的路径
SELECT
    snick, department_name
FROM (
    SELECT snick, department_id
    FROM ods.xinghuan_employee_snick_all
    WHERE day = toYYYYMMDD(yesterday())
    -- 组织架构包含店铺
    AND mp_shop_id GLOBAL IN (
        SELECT department_id AS shop_id
        FROM xqc_dim.group_all
        WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        AND is_shop = 'True'
        AND platform = '{{ platform=tb }}'
    )

    /* -- 已订阅店铺
    -- PS: 和组织架构所包含店铺二选一
    AND mp_shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        AND platform = '{{ platform=tb }}'
    ) */
    -- 权限隔离
    AND (
            mp_shop_id IN splitByChar(',','{{ shop_id_list=615faf72b0c5f1001957c249 }}')
            OR
            snick IN splitByChar(',','{{ snick_list=NULL }}')
        )
) snick_department_map
GLOBAL LEFT JOIN (
    -- PS: 此处需要JOIN 2次来获取子账号分组的路径, 目前仅JOIN了一次
    SELECT
        level_2.department_id AS department_id,
        if(
            level_2.department_name!='', 
            concat(level_1.department_name,'-',level_2.department_name)
        ) AS department_name
    FROM (
        SELECT 
            _id AS department_id,
            name AS department_name
        FROM ods.xinghuan_department_all
        WHERE day = toYYYYMMDD(yesterday())
        AND company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
    ) AS level_1
    GLOBAL LEFT JOIN (
        SELECT 
            _id AS department_id,
            name AS department_name,
            parent_id AS parent_department_id
        FROM ods.xinghuan_department_all
        WHERE day = toYYYYMMDD(yesterday())
        AND company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
    ) AS level_2
    ON level_1.department_id = level_2.parent_department_id
    AND level_1.department_id != level_2.department_id

) AS department_info
USING department_id

-- 3. 将子账号统计结果附加到分组信息熵