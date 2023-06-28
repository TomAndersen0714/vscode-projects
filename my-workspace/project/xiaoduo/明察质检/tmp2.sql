orderid
shopid
buyerid
realbuyernick
payment
status
updatedat
itemids
steptradestatus
steppaidfee


CREATE TABLE test.t2 ( `day` Int32, `name` String )
ENGINE = MergeTree
ORDER BY name
SETTINGS index_granularity = 8192, storage_policy = 'rr'
