-- 子账号分组会话量

-- 1. 统计各个子账号当天会话量
-- PS: 目前缺少子账号和对应分组的映射, 需要先修改表结构, 然后从融合版迁移维度表历史数据
-- 到老淘宝, 以及拉取最新的维度数据 ods.xinghuan_employee_snick_all
WITH
(SELECT toYYYYMMDD(today())) AS today,
(
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day = today

    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        AND platform = '{{ platform=tb }}'
    )
) AS today_dialog_cnt -- 当天目前已有会话总量
SELECT
    snick, -- 子账号名
    COUNT(1) AS snick_today_dialog_cnt -- 子账号当天会话量
FROM xqc_ods.dialog_all
WHERE day = today

-- 已订阅店铺
AND shop_id GLOBAL IN (
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    AND platform = '{{ platform=tb }}'
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

    -- 已订阅店铺
    AND mp_shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        AND platform = '{{ platform=tb }}'
    )
) snick_department_id_map
GLOBAL LEFT JOIN (
    -- PS: 此处需要JOIN 3次来获取子账号分组的完整路径, 因为子账号分组树高为4
    SELECT
        level_1.parent_department_id AS parent_department_id, -- parent_department_id全为空,则代表树层次遍历完毕
        level_2_3_4.department_id AS department_id,
        if(
            level_1.department_id!='', 
            concat(level_1.department_name,'-',level_2_3_4.department_name),
            level_2_3_4.department_name
        ) AS department_name
    FROM (
        SELECT
            level_2.parent_department_id AS parent_department_id,
            level_3_4.department_id AS department_id,
            if(
                level_2.department_id!='', 
                concat(level_2.department_name,'-',level_3_4.department_name),
                level_3_4.department_name
            ) AS department_name
        FROM (
            SELECT
                level_3.parent_department_id AS parent_department_id,
                level_4.department_id AS department_id,
                if(
                    level_3.department_id!='', 
                    concat(level_3.department_name,'-',level_4.department_name),
                    level_4.department_name
                ) AS department_name
            FROM (
                SELECT 
                    _id AS department_id,
                    name AS department_name,
                    parent_id AS parent_department_id
                FROM ods.xinghuan_department_all
                WHERE day = toYYYYMMDD(yesterday())
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            ) AS level_4
            GLOBAL LEFT JOIN (
                SELECT 
                    _id AS department_id,
                    name AS department_name,
                    parent_id AS parent_department_id
                FROM ods.xinghuan_department_all
                WHERE day = toYYYYMMDD(yesterday())
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            ) AS level_3
            ON level_4.parent_department_id = level_3.department_id
        ) AS level_3_4
        GLOBAL LEFT JOIN (
            SELECT 
                _id AS department_id,
                name AS department_name,
                parent_id AS parent_department_id
            FROM ods.xinghuan_department_all
            WHERE day = toYYYYMMDD(yesterday())
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        ) AS level_2
        ON level_3_4.parent_department_id = level_2.department_id
    ) AS level_2_3_4
    GLOBAL LEFT JOIN (
        SELECT 
            _id AS department_id,
            name AS department_name,
            parent_id AS parent_department_id
        FROM ods.xinghuan_department_all
        WHERE day = toYYYYMMDD(yesterday())
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    ) AS level_1
    ON level_2_3_4.parent_department_id = level_1.department_id
) AS department_info
USING department_id

-- 3. 将子账号统计结果附加到分组信息中
WITH
(SELECT toYYYYMMDD(today())) AS today,
(
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day = today

    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        AND platform = '{{ platform=tb }}'
    )
) AS today_dialog_cnt -- 当天目前已有会话总量
SELECT
    department_name,
    sum(snick_today_dialog_cnt) AS department_dialog_cnt,
    if(
        today_dialog_cnt != 0, round(department_dialog_cnt/today_dialog_cnt,2), 0.00
    ) AS department_dialog_cnt_percent -- 分组当天会话量占比
FROM (
    -- 1. 统计各个子账号当天会话量
    SELECT
        snick, -- 子账号名
        COUNT(1) AS snick_today_dialog_cnt -- 子账号当天会话量
    FROM xqc_ods.dialog_all
    WHERE day = today

    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        AND platform = '{{ platform=tb }}'
    )
    GROUP BY snick
)
GLOBAL LEFT JOIN (
    -- 2. 获取子账号与子账号分组的T+1映射
    -- PS: 由于子账号分组最多有3层, 因此需要进行2次JOIN, 来获取子账号分组的路径
    SELECT
        snick, department_name
    FROM (
        SELECT snick, department_id
        FROM ods.xinghuan_employee_snick_all
        WHERE day = toYYYYMMDD(yesterday())

        -- 已订阅店铺
        AND mp_shop_id GLOBAL IN (
            SELECT tenant_id AS shop_id
            FROM xqc_dim.company_tenant
            WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            AND platform = '{{ platform=tb }}'
        )
    ) snick_department_id_map
    GLOBAL LEFT JOIN (
        -- PS: 此处需要JOIN 3次来获取子账号分组的完整路径, 因为子账号分组树高为4
        -- parent_department_id全为空,则代表树层次遍历完毕
        SELECT
            level_1.parent_department_id AS parent_department_id,
            level_2_3_4.department_id AS department_id,
            if(
                level_1.department_id!='', 
                concat(level_1.department_name,'-',level_2_3_4.department_name),
                level_2_3_4.department_name
            ) AS department_name
        FROM (
            SELECT
                level_2.parent_department_id AS parent_department_id,
                level_3_4.department_id AS department_id,
                if(
                    level_2.department_id!='', 
                    concat(level_2.department_name,'-',level_3_4.department_name),
                    level_3_4.department_name
                ) AS department_name
            FROM (
                SELECT
                    level_3.parent_department_id AS parent_department_id,
                    level_4.department_id AS department_id,
                    if(
                        level_3.department_id!='', 
                        concat(level_3.department_name,'-',level_4.department_name),
                        level_4.department_name
                    ) AS department_name
                FROM (
                    SELECT 
                        _id AS department_id,
                        name AS department_name,
                        parent_id AS parent_department_id
                    FROM ods.xinghuan_department_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND company_id = '{{ company_id }}'
                    AND (
                        parent_id GLOBAL IN (
                            SELECT DISTINCT
                                _id AS department_id
                            FROM ods.xinghuan_department_all
                            WHERE day = toYYYYMMDD(yesterday())
                            AND company_id = '{{ company_id }}'
                        ) -- 清除子账号父分组被删除, 而子分组依旧存在的脏数据
                        OR 
                        parent_id = '' -- 保留顶级分组
                    )
                ) AS level_4
                GLOBAL LEFT JOIN (
                    SELECT 
                        _id AS department_id,
                        name AS department_name,
                        parent_id AS parent_department_id
                    FROM ods.xinghuan_department_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND company_id = '{{ company_id }}'
                ) AS level_3
                ON level_4.parent_department_id = level_3.department_id
            ) AS level_3_4
            GLOBAL LEFT JOIN (
                SELECT 
                    _id AS department_id,
                    name AS department_name,
                    parent_id AS parent_department_id
                FROM ods.xinghuan_department_all
                WHERE day = toYYYYMMDD(yesterday())
                AND company_id = '{{ company_id }}'
            ) AS level_2
            ON level_3_4.parent_department_id = level_2.department_id
        ) AS level_2_3_4
        GLOBAL LEFT JOIN (
            SELECT 
                _id AS department_id,
                name AS department_name,
                parent_id AS parent_department_id
            FROM ods.xinghuan_department_all
            WHERE day = toYYYYMMDD(yesterday())
            AND company_id = '{{ company_id }}'
        ) AS level_1
        ON level_2_3_4.parent_department_id = level_1.department_id
    ) AS department_info
    USING department_id
) AS snick_department_name_map
USING snick
GROUP BY department_name
ORDER BY department_dialog_cnt DESC
LIMIT 10

