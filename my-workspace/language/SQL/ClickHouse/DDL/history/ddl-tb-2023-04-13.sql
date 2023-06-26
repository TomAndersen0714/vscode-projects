-- ALTER TABLE ft_dim.main_goods_info_local ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `spu_id`
ALTER TABLE ft_dim.main_goods_info_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `spu_id` String AFTER `shop_name`;

-- ALTER TABLE ft_dim.main_goods_info_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `spu_id`
ALTER TABLE ft_dim.main_goods_info_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `spu_id` String AFTER `shop_name`;