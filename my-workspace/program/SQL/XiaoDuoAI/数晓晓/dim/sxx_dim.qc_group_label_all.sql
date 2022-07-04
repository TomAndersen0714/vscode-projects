CREATE DATABASE sxx_dim ON CLUSTER cluster_3s_2r

-- DROP TABLE sxx_dim.qc_group_label_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_dim.qc_group_label_local ON CLUSTER cluster_3s_2r
(
    `snapshot_day` Int32,
    `create_time` String,
    `update_time` String,
    `platform` String,
    `company_id` String,
    `qc_norm_id` String,
    `group_id` String,
    `group_name` String,
    `sub_group_id` String,
    `sub_group_name` String,
    `sub_sub_group_id` String,
    `sub_sub_group_name` String,
    `full_group_name` String,
    `qc_label_id` String,
    `qc_label_name` String,
    `qc_label_category` String,
    `responsible_party` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY (snapshot_day)
ORDER BY (platform, qc_label_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE sxx_dim.qc_group_label_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_dim.qc_group_label_all ON CLUSTER cluster_3s_2r
AS sxx_dim.qc_group_label_local
ENGINE = Distributed('cluster_3s_2r', 'sxx_dim', 'qc_group_label_local', rand())

-- DROP TABLE buffer.sxx_ods_qc_group_label_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.sxx_ods_qc_group_label_buffer ON CLUSTER cluster_3s_2r
AS sxx_dim.qc_group_label_all
ENGINE = Buffer('sxx_dim', 'qc_group_label_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)


-- ALTER TABLE sxx_dim.qc_group_label_local ON CLUSTER cluster_3s_2r DROP PARTITION {snapshot_ds_nodash}
INSERT INTO sxx_dim.qc_group_label_all
SELECT
    {snapshot_ds_nodash} AS snapshot_day,
    voc_group_label_info.*,
    responsible_party_map.responsible_party AS responsible_party
FROM (
    SELECT
        voc_group_info.*,
        voc_label_info._id AS qc_label_id,
        voc_label_info.name AS qc_label_name
    FROM (
        SELECT
            voc_sub_group.*,
            voc_sub_sub_group._id AS sub_sub_group_id,
            voc_sub_sub_group.name AS sub_sub_group_name,
            concat(
                sub_group_name,
                if(sub_sub_group_name='', '', '/'),
                if(sub_sub_group_name='', '', sub_sub_group_name)
            ) AS full_group_name
        FROM (
            SELECT
                voc_group.*,
                voc_sub_group._id AS sub_group_id,
                voc_sub_group.name AS sub_group_name
            FROM (
                -- 查询VOC质检项一级分组
                SELECT
                    qc_norm_group.create_time,
                    qc_norm_group.update_time,
                    qc_norm.platform,
                    qc_norm.company_id,
                    qc_norm_group.qc_norm_id,
                    qc_norm_group._id AS group_id,
                    qc_norm_group.name AS group_name
                FROM (
                    SELECT *
                    FROM xqc_dim.qc_norm_group_all
                    WHERE day = {snapshot_ds_nodash}
                    AND qc_norm_id GLOBAL IN (
                        SELECT _id
                        FROM ods.xinghuan_qc_norm_all
                        WHERE day = {snapshot_ds_nodash}
                        AND company_id = '61ea1e600e41b86080fcea99'
                        AND status = 1
                    )
                    -- 字符串匹配获取分组
                    AND name LIKE '%voc%'
                ) AS qc_norm_group
                GLOBAL INNER JOIN (
                    SELECT *
                    FROM ods.xinghuan_qc_norm_all
                    WHERE day = {snapshot_ds_nodash}
                    AND company_id = '61ea1e600e41b86080fcea99'
                ) AS qc_norm
                ON qc_norm_group.qc_norm_id = qc_norm._id
            ) AS voc_group
            GLOBAL LEFT JOIN (
                -- 关联VOC质检项二级分组
                SELECT *
                FROM xqc_dim.qc_norm_group_all
                WHERE day = {snapshot_ds_nodash}
                AND qc_norm_id GLOBAL IN (
                    SELECT _id
                    FROM ods.xinghuan_qc_norm_all
                    WHERE day = {snapshot_ds_nodash}
                    AND company_id = '61ea1e600e41b86080fcea99'
                    AND status = 1
                )
            ) AS voc_sub_group
            ON voc_group.group_id = voc_sub_group.parent_id
        ) AS voc_sub_group
        GLOBAL LEFT JOIN (
            -- 关联VOC质检项三级分组
            SELECT *
            FROM xqc_dim.qc_norm_group_all
            WHERE day = {snapshot_ds_nodash}
            AND qc_norm_id GLOBAL IN (
                SELECT _id
                FROM ods.xinghuan_qc_norm_all
                WHERE day = {snapshot_ds_nodash}
                AND company_id = '61ea1e600e41b86080fcea99'
                AND status = 1
            )
        ) AS voc_sub_sub_group
        ON voc_sub_group.sub_group_id = voc_sub_sub_group.parent_id
    ) AS voc_group_info
    GLOBAL LEFT JOIN (
        -- 关联VOC质检项
        SELECT
            qc_norm_group_id,
            _id,
            name
        FROM xqc_dim.qc_rule_all
        WHERE day = {snapshot_ds_nodash}
        AND company_id = '61ea1e600e41b86080fcea99'
        AND status = 1
    ) AS voc_label_info
    ON voc_group_info.sub_sub_group_id = voc_label_info.qc_norm_group_id
) AS voc_group_label_info
GLOBAL LEFT JOIN (
    -- 关联VOC质检项分组责任方
    SELECT
        qc_label_group_name,
        qc_label_sub_group_name,
        responsible_party
    FROM sxx_dim.responsible_party_map_all
    WHERE snapshot_day = {snapshot_ds_nodash}
) AS responsible_party_map
ON voc_group_label_info.sub_group_name = responsible_party_map.qc_label_group_name
AND voc_group_label_info.sub_sub_group_name = responsible_party_map.qc_label_sub_group_name

