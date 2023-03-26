-- DROP TABLE IF EXISTS test.mv_source
CREATE TABLE test.mv_source
(
    `sign` String,
    `uuid` String
)
ENGINE = MergeTree()
ORDER BY sign;


-- DROP TABLE IF EXISTS test.mv_sink_stat
CREATE TABLE test.mv_sink_stat
(
    `sign` String,
    `uuid_uniq_state` AggregateFunction(uniq, String)
)
ENGINE = MergeTree()
ORDER BY sign;


-- DROP TABLE IF EXISTS test.mv
CREATE MATERIALIZED VIEW test.mv
TO test.mv_sink_stat
AS
SELECT
    sign,
    uniqState(uuid) AS uuid_uniq_state
FROM test.mv_source
GROUP BY sign;


-- INSERT INTO
INSERT INTO test.mv_source(`sign`, `uuid`)
VALUES ('a', '001');

INSERT INTO test.mv_source(`sign`, `uuid`)
VALUES ('a', '001'), ('a', '002'), ('c', '003');


-- SELECT
SELECT
    sign,
    uniqMerge(uuid_uniq_state)
FROM test.mv_sink_stat
GROUP BY sign;