ALTER TABLE ft_dwd.order_detail_local ON CLUSTER cluster_3s_2r
RENAME COLUMN original_sratus TO original_status

ALTER TABLE ft_dwd.order_detail_all ON CLUSTER cluster_3s_2r
RENAME COLUMN original_sratus TO original_status