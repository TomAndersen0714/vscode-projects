-- GoLang
type TransferMsg struct {
   Platform   string    `json:"platform"`
   PlatUserID string    `json:"plat_user_id"`
   FromSpin   string    `json:"from_spin"`
   ToSpin     string    `json:"to_spin"`
   Pin        string    `json:"pin"`
   BuyerOneID string    `json:"buyer_one_id"`
   CreateTime time.Time `json:"create_time"`
}

CREATE DATABASE IF NOT EXISTS ods ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE ods.transfer_msg_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ods.transfer_msg_local ON CLUSTER cluster_3s_2r
(
    `platform` String,
    `plat_user_id` String,
    `from_spin` String,
    `to_spin` String,
    `pin` String,
    `buyer_one_id` String,
    `create_time` String,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, plat_user_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE ods.transfer_msg_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ods.transfer_msg_all ON CLUSTER cluster_3s_2r
AS ods.transfer_msg_local
ENGINE = Distributed('cluster_3s_2r', 'ods', 'transfer_msg_local', rand())

-- DROP TABLE buffer.ods_transfer_msg_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.ods_transfer_msg_buffer ON CLUSTER cluster_3s_2r
AS ods.transfer_msg_all
ENGINE = Buffer('ods', 'transfer_msg_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)

-- INSERT INTO
INSERT INTO buffer.ods_transfer_msg_buffer
SELECT *
FROM remote('10.22.113.168:19000', ods.transfer_msg_all)
WHERE day BETWEEN 20220904 AND 20220910