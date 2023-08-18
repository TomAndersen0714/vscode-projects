CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS test.data_sync;

CREATE TABLE test.data_sync (
    integer_type INTEGER,
    bigint_type BIGINT,
    tiny_int TINYINT,
    smallint_type SMALLINT,
    int_type INT,
    bool_type BOOLEAN,
    float_type FLOAT,
    double_type DOUBLE,
    string_type STRING,
    time_type TIMESTAMP,
    varchar_type VARCHAR,
    PRIMARY KEY (integer_type)
)
PARTITION BY HASH(integer_type) PARTITIONS 4
STORED AS KUDU 
TBLPROPERTIES (
    'kudu.master_addresses' = 'cdh0,cdh1,cdh2'
);

UPSERT INTO test.data_sync VALUES(
    1,1,1,1,1,True,1.2,1.1,'Tom','2021-08-06','Andersen'
)

UPSERT INTO test.data_sync VALUES(
    2,1,1,1,1,True,1.2,1.1,'Tom','2021-08-06','Andersen'
)