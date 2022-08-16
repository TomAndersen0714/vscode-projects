CREATE DATABASE IF NOT EXISTS xqc_dim ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE xqc_dim.snick_full_info_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.snick_full_info_local ON CLUSTER cluster_3s_2r
(
    `company_id` String,
    `platform` String,
    `shop_id` String,
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
SELECT
    company_id, platform, shop_id,
    department_id, department_name, snick, employee_id, employee_name, 
    superior_id, superior_name,
    day
FROM (
    SELECT
        *
    FROM (
        SELECT
            day,
            company_id,
            platform,
            mp_shop_id AS shop_id,
            department_id,
            employee_id,
            snick
        FROM ods.xinghuan_employee_snick_all
        WHERE day = {snapshot_ds_nodash}
    ) AS snick_info
    GLOBAL LEFT JOIN (
        SELECT
            _id AS employee_id,
            username AS employee_name,
            superior_id,
            superior_name
        FROM ods.xinghuan_employee_all
        WHERE day = {snapshot_ds_nodash}
    ) AS employee_info
    USING(employee_id)
) AS snick_employee_info
GLOBAL LEFT JOIN (
    SELECT
        _id AS department_id,
        full_name AS department_name
    FROM xqc_dim.snick_department_full_all
    WHERE day = {snapshot_ds_nodash}
) AS department_info
USING (department_id)