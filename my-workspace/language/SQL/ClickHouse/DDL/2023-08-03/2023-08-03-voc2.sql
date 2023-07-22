-- RENAME TABLE
-- dwd.voc_buyer_recent_order_local TO dwd.voc_buyer_latest_order_local
-- ON CLUSTER cluster_3s_2r;
RENAME TABLE
dwd.voc_buyer_latest_order_local TO dwd.voc_buyer_recent_order_local
ON CLUSTER cluster_3s_2r;


-- DROP TABLE dwd.voc_buyer_recent_order_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dwd.voc_buyer_recent_order_all ON CLUSTER cluster_3s_2r
AS dwd.voc_buyer_recent_order_local
ENGINE = Distributed('cluster_3s_2r', 'dwd', 'voc_buyer_recent_order_local', rand());