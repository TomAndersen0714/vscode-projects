-- DROP TABLE test.visits
CREATE TABLE test.visits
(
    StartDate DateTime64,
    CounterID UInt64,
    Sign Int32,
    UserID Int32
) ENGINE = MergeTree ORDER BY (StartDate, CounterID);


-- DROP TABLE test.mv_visits
CREATE MATERIALIZED VIEW test.mv_visits
(
    StartDate DateTime64,
    CounterID UInt64,
    Visits AggregateFunction(sum, Int32),
    Users AggregateFunction(uniq, Int32)
)
ENGINE = AggregatingMergeTree() ORDER BY (StartDate, CounterID)
AS SELECT
    StartDate,
    CounterID,
    sumState(Sign) AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY StartDate, CounterID;

-- INSERT
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID) VALUES (1667446031, 1, 3, 4);
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID) VALUES (1667446031, 1, 6, 3);


-- SELECT
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.mv_visits
GROUP BY StartDate
ORDER BY StartDate;