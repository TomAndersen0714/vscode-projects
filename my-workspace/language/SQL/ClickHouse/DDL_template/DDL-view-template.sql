CREATE DATABASE IF NOT EXISTS xqc_dim ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE xqc_dim.shop_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE VIEW IF NOT EXISTS xqc_dim.shop_all ON CLUSTER cluster_3s_2r
AS
SELECT *
FROM xqc_dim.xqc_shop_all
WHERE day = toYYYYMMDD(yesterday())