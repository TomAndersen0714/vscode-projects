CREATE DATABASE IF NOT EXISTS xqc_dim ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE xqc_dim.snick_full_info_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.snick_full_info_local ON CLUSTER cluster_3s_2r
(
    `company_id` String,
    `company_name` String,
    `company_short_name` String,
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `seller_nick` String,
    `department_id` String,
    `department_name` String,
    `snick` String,
    `employee_id` String,
    `employee_name` String,
    `superior_id` String,
    `superior_name` String,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (company_id, platform)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_dim.snick_full_info_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.snick_full_info_all ON CLUSTER cluster_3s_2r
AS xqc_dim.snick_full_info_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'snick_full_info_local', rand())

-- INSERT INTO
INSERT INTO {sink_tbl}
SELECT
    company_id, company_name, company_short_name, platform,
    shop_id, shop_name, seller_nick,
    department_id, department_name,
    snick, employee_id, employee_name, superior_id, superior_name,
    day
FROM (
    SELECT
        company_id, platform,
        shop_id, shop_name, seller_nick,
        department_id, department_name,
        snick, employee_id, employee_name, superior_id, superior_name,
        day
    FROM (
        SELECT
            company_id, platform,
            shop_id,
            department_id, department_name,
            snick, employee_id, employee_name, superior_id, superior_name,
            day
        FROM (
            SELECT
                *
            FROM (
                SELECT
                    company_id, platform,
                    mp_shop_id AS shop_id,
                    department_id,
                    snick, employee_id,
                    day
                FROM {snick_info_src_tbl}
                WHERE day = {snapshot_ds_nodash}
            ) AS snick_info
            GLOBAL LEFT JOIN (
                SELECT DISTINCT
                    company_id,
                    _id AS employee_id,
                    username AS employee_name,
                    superior_id,
                    superior_name
                FROM {employee_info_src_tbl}
                WHERE day = {snapshot_ds_nodash}
            ) AS employee_info
            USING(company_id, employee_id)
        ) AS snick_employee_info
        GLOBAL LEFT JOIN (
            SELECT DISTINCT
                company_id,
                _id AS department_id,
                full_name AS department_name
            FROM {department_info_src_tbl}
            WHERE day = {snapshot_ds_nodash}
        ) AS department_info
        USING (company_id, department_id)
    ) AS snick_employee_department_info
    GLOBAL LEFT JOIN (
        SELECT DISTINCT
            company_id,
            shop_id,
            seller_nick,
            plat_shop_name AS shop_name
        FROM {shop_info_src_tbl}
        WHERE day = {snapshot_ds_nodash}
    ) AS shop_info
    USING(company_id, shop_id)
) AS snick_employee_department_shop_info
GLOBAL LEFT JOIN (
    SELECT DISTINCT
        _id AS company_id,
        name AS company_name,
        shot_name AS company_short_name
    FROM {company_info_src_tbl}
    WHERE day = {snapshot_ds_nodash}
) AS company_info
USING(company_id)
