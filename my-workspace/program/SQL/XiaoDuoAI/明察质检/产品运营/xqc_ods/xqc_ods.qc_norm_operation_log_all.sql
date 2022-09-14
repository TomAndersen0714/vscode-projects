-- JSON Example
{
  _id: ObjectId("62a878e26741dc36322ba1c1"),
  operator: {
    _id: ObjectId("5f73e9c1684bf7000141363d"),
    name: '测试',
    platform: 'jd',
    company_id: ObjectId("5f73e9c1684bf70001413636")
  },
  action: {
    verb: 2,
    noun: 'qc_rule',
    object: {
      id: '61b6f554130c40817c387b34',
      name: '错别字7',
      from: {
        qc_norm_name: '淘系规则',
        qc_norm_id: ObjectId("61b6f554130c40817c387ae2")
      },
      condition: Binary(Buffer.from("16000000075f69640061b6f554130c40817c387b3400", "hex"), 0),
      operation: Binary(Buffer.from("ad000000032473657400a200000008636865636b000110616c6572745f6c6576656c0000000000106e6f746966795f7761790002000000106e6f746966795f7461726765740001000000017468726573686f6c6400000000000000000010636865636b5f74617267657400010000001073636f726500f6ffffff097570646174655f74696d65008e34186281010000077570646174655f6163636f756e74005f73e9c1684bf7000141363e0000", "hex"), 0),
      raw_data: Binary(Buffer.from("7b226964223a22363162366635353431333063343038313763333837623334222c226372656174655f74696d65223a22323032322d30332d33315431393a32313a34302e39335a222c227570646174655f74696d65223a22323032322d30332d33315431393a32313a34302e39335a222c22636f6d70616e795f6964223a22356637336539633136383462663730303031343133363336222c22706c6174666f726d223a226a64222c2271635f6e6f726d5f6964223a22363162366635353431333063343038313763333837616532222c2271635f6e6f726d5f67726f75705f6964223a22363162366635353431333063343038313763333837623333222c2274656d706c6174655f6964223a22363234356666383332653631656566376639356234633361222c226e616d65223a22e99499e588abe5ad9737222c2273656c6c65725f6e69636b223a22222c2272756c655f63617465676f7279223a322c2272756c655f74797065223a302c2273657474696e6773223a6e756c6c2c22636865636b223a66616c73652c22636865636b5f746172676574223a312c22616c6572745f6c6576656c223a302c226e6f746966795f776179223a322c226e6f746966795f746172676574223a312c2273636f7265223a2d31302c227468726573686f6c64223a302c227370656369616c5f73657474696e6773223a6e756c6c2c22537461747573223a317d", "hex"), 0),
      diff_data: Binary(Buffer.from("7b22636865636b223a747275652c227570646174655f74696d65223a22323032322d30362d31345431323a30323a34322e3434365a227d", "hex"), 0),
      ext: null
    },
    field: 'qc_norm/qc_rule'
  },
  create_time: Long("1655208162"),
  cause: null
}

CREATE DATABASE IF NOT EXISTS xqc_ods ON CLUSTER cluster_3s_2r

-- DROP TABLE xqc_ods.qc_norm_operation_log_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ods.qc_norm_operation_log_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `platform` String,
    `company_id` String,
    `operator_id` String,
    `operator_name` String,
    `action_verb` String,
    `action_noun` String,
    `object_id` String,
    `object_name` String,
    `create_time` Int64,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_ods.qc_norm_operation_log_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ods.qc_norm_operation_log_all ON CLUSTER cluster_3s_2r
AS xqc_ods.qc_norm_operation_log_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'qc_norm_operation_log_local', rand())

-- DROP TABLE buffer.xqc_ods_qc_norm_operation_log_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xqc_ods_qc_norm_operation_log_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.qc_norm_operation_log_all
ENGINE = Buffer('xqc_ods', 'qc_norm_operation_log_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)